package conformance

import (
	"bytes"
	"crypto/rand"
	"crypto/sha256"
	_ "embed"
	"encoding/hex"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/BoltzExchange/boltz-client/v2/pkg/boltz"
	"github.com/btcsuite/btcd/btcec/v2"
	"github.com/btcsuite/btcd/btcec/v2/ecdsa"
	"github.com/btcsuite/btcd/btcutil"
	"github.com/btcsuite/btcd/chaincfg/chainhash"
	"github.com/btcsuite/btcd/txscript"
	"github.com/btcsuite/btcd/wire"
	"github.com/lightningnetwork/lnd/lnwire"
	"github.com/lightningnetwork/lnd/zpay32"
)

//go:embed boltz-vectors.json
var vectorJSON []byte

type vector struct {
	ProviderPublicKey  string               `json:"providerPublicKey"`
	Preimage           string               `json:"preimage"`
	PreimageHash       string               `json:"preimageHash"`
	TimeoutBlockHeight uint32               `json:"timeoutBlockHeight"`
	SwapTree           boltz.SerializedTree `json:"swapTree"`
	Address            string               `json:"address"`
	ClaimControlBlock  string               `json:"claimControlBlock"`
	RefundControlBlock string               `json:"refundControlBlock"`
}

func check(t *testing.T, err error) {
	t.Helper()
	if err != nil {
		t.Fatal(err)
	}
}

func unhex(t *testing.T, value string) []byte {
	t.Helper()
	result, err := hex.DecodeString(value)
	check(t, err)
	return result
}

func clientKey() *btcec.PrivateKey {
	scalar := make([]byte, 32)
	scalar[31] = 2
	privateKey, _ := btcec.PrivKeyFromBytes(scalar)
	return privateKey
}

func verifiedTree(t *testing.T, serialized *boltz.SerializedTree, provider []byte, swapType boltz.SwapType, height uint32, hash []byte, address string) *boltz.SwapTree {
	t.Helper()
	if serialized == nil {
		t.Fatal("server omitted swapTree")
	}
	providerKey, err := btcec.ParsePubKey(provider)
	check(t, err)
	tree := serialized.Deserialize()
	check(t, tree.Init(boltz.CurrencyBtc, swapType == boltz.ReverseSwap, clientKey(), providerKey))
	check(t, tree.Check(swapType, height, hash))
	check(t, tree.CheckAddress(address, boltz.Regtest, nil))
	return tree
}

func TestOfficialTaprootVectors(t *testing.T) {
	var fixture struct {
		Cases map[string]vector `json:"cases"`
	}
	check(t, json.Unmarshal(vectorJSON, &fixture))
	if len(fixture.Cases) != 2 {
		t.Fatal("expected both swap directions")
	}
	for name, v := range fixture.Cases {
		t.Run(name, func(t *testing.T) {
			swapType := boltz.SwapType(name)
			tree := verifiedTree(t, &v.SwapTree, unhex(t, v.ProviderPublicKey), swapType, v.TimeoutBlockHeight, unhex(t, v.PreimageHash), v.Address)
			for refund, expected := range map[bool]string{false: v.ClaimControlBlock, true: v.RefundControlBlock} {
				control, err := tree.GetControlBlock(refund)
				check(t, err)
				if hex.EncodeToString(control) != expected {
					t.Fatal("control block disagrees with official reference library")
				}
			}
			preimage := unhex(t, v.Preimage)
			if swapType == boltz.NormalSwap {
				preimage = nil
			}
			output := spendOutput(t, tree, v.Address, swapType, preimage, v.TimeoutBlockHeight)
			spend := constructSpend(t, output, nil)
			verifySpend(t, output, spend)
			witness := spend.MsgTx().TxIn[0].Witness
			expectedItems := 4
			if swapType == boltz.NormalSwap {
				expectedItems = 3
				if spend.MsgTx().LockTime != v.TimeoutBlockHeight {
					t.Fatal("refund omitted CLTV locktime")
				}
			}
			if len(witness) != expectedItems {
				t.Fatal("expected a unilateral script-path witness")
			}
			// The same transaction must fail when a required witness value is changed.
			invalid := spend.MsgTx().Copy()
			if swapType == boltz.ReverseSwap {
				invalid.TxIn[0].Witness[1] = make([]byte, 32)
			} else {
				earlyRefund := output
				earlyRefund.TimeoutBlockHeight--
				invalid = constructSpend(t, earlyRefund, nil).MsgTx()
			}
			if executeSpend(output, invalid) == nil {
				t.Fatal("invalid unilateral spend was accepted")
			}
		})
	}
}

func spendOutput(t *testing.T, tree *boltz.SwapTree, address string, swapType boltz.SwapType, preimage []byte, height uint32) boltz.OutputDetails {
	t.Helper()
	lockAddress, err := btcutil.DecodeAddress(address, boltz.Regtest.Btc)
	check(t, err)
	script, err := txscript.PayToAddrScript(lockAddress)
	check(t, err)
	lock := wire.NewMsgTx(2)
	lock.AddTxIn(wire.NewTxIn(&wire.OutPoint{Hash: chainhash.Hash{1}, Index: 0}, nil, nil))
	lock.AddTxOut(wire.NewTxOut(100_000, script))
	lockup := &boltz.BtcTransaction{Tx: *btcutil.NewTx(lock)}
	timeout := uint32(0)
	if swapType == boltz.NormalSwap {
		timeout = height
	}
	return boltz.OutputDetails{LockupTransaction: lockup, Vout: 0, Address: boltz.Regtest.DummyLockupAddress[boltz.CurrencyBtc], PrivateKey: clientKey(), Preimage: preimage, TimeoutBlockHeight: timeout, SwapTree: tree, Cooperative: false, SwapId: "fixture", SwapType: swapType}
}

func constructSpend(t *testing.T, output boltz.OutputDetails, api *boltz.Api) *boltz.BtcTransaction {
	t.Helper()
	feeRate := 1.0
	spend, results, err := boltz.ConstructTransaction(boltz.Regtest, boltz.CurrencyBtc, []boltz.OutputDetails{output}, boltz.Fee{SatsPerVbyte: &feeRate}, api)
	check(t, err)
	check(t, results[output.SwapId].Err)
	transaction, ok := spend.(*boltz.BtcTransaction)
	if !ok {
		t.Fatal("expected Bitcoin transaction")
	}
	return transaction
}

func executeSpend(output boltz.OutputDetails, spend *wire.MsgTx) error {
	previous := output.LockupTransaction.(*boltz.BtcTransaction).MsgTx().TxOut[0]
	fetcher := txscript.NewCannedPrevOutputFetcher(previous.PkScript, previous.Value)
	hashes := txscript.NewTxSigHashes(spend, fetcher)
	engine, err := txscript.NewEngine(previous.PkScript, spend, 0, txscript.StandardVerifyFlags, nil, hashes, previous.Value, fetcher)
	if err != nil {
		return err
	}
	return engine.Execute()
}

func verifySpend(t *testing.T, output boltz.OutputDetails, spend *boltz.BtcTransaction) {
	t.Helper()
	check(t, executeSpend(output, spend.MsgTx()))
}

type countingTransport struct{ claimCalls atomic.Int32 }

func (transport *countingTransport) RoundTrip(request *http.Request) (*http.Response, error) {
	if request.Method == http.MethodPost && strings.HasSuffix(request.URL.Path, "/claim") {
		transport.claimCalls.Add(1)
	}
	return http.DefaultTransport.RoundTrip(request)
}

func testInvoice(t *testing.T, hash [32]byte) string {
	t.Helper()
	invoice, err := zpay32.NewInvoice(boltz.Regtest.Btc, hash, time.Now(), zpay32.Amount(lnwire.MilliSatoshi(100_000_000)), zpay32.Description("Pubky Swap interoperability test"), zpay32.Expiry(time.Hour),
		zpay32.PaymentAddr([32]byte{9}),
		zpay32.Features(lnwire.NewFeatureVector(lnwire.NewRawFeatureVector(lnwire.TLVOnionPayloadOptional, lnwire.PaymentAddrOptional), lnwire.Features)))
	check(t, err)
	encoded, err := invoice.Encode(zpay32.MessageSigner{SignCompact: func(message []byte) ([]byte, error) {
		return ecdsa.SignCompact(clientKey(), chainhash.HashB(message), true), nil
	}})
	check(t, err)
	return encoded
}

func TestProxyOfficialSDK(t *testing.T) {
	endpoint := os.Getenv("PUBKY_BOLTZ_TEST_URL")
	if endpoint == "" {
		t.Skip("set PUBKY_BOLTZ_TEST_URL to the Rust fixture server URL")
	}
	transport := &countingTransport{}
	api := &boltz.Api{URL: strings.TrimRight(endpoint, "/"), Client: http.Client{Timeout: 10 * time.Second, Transport: transport}}
	version, err := api.GetVersion()
	check(t, err)
	if version.Version == "" {
		t.Fatal("missing proxy version")
	}
	fee, err := api.GetFeeEstimation(boltz.CurrencyBtc)
	check(t, err)
	if fee <= 0 {
		t.Fatal("invalid fee estimate")
	}
	height, err := api.GetBlockHeight(boltz.CurrencyBtc)
	check(t, err)
	if height == 0 {
		t.Fatal("invalid chain height")
	}

	submarinePairs, err := api.GetSubmarinePairs()
	check(t, err)
	submarinePair, ok := submarinePairs[boltz.CurrencyBtc][boltz.CurrencyBtc]
	if !ok {
		t.Fatal("missing BTC submarine discovery pair")
	}
	reversePairs, err := api.GetReversePairs()
	check(t, err)
	reversePair, ok := reversePairs[boltz.CurrencyBtc][boltz.CurrencyBtc]
	if !ok {
		t.Fatal("missing BTC reverse discovery pair")
	}
	chainPairs, err := api.GetChainPairs()
	check(t, err)
	if len(chainPairs) != 0 {
		t.Fatal("unexpected advertised chain swaps")
	}

	preimage := make([]byte, 32)
	_, err = rand.Read(preimage)
	check(t, err)
	hash := sha256.Sum256(preimage)
	submarineHash := sha256.Sum256(hash[:])
	publicKey := clientKey().PubKey().SerializeCompressed()
	sub, err := api.CreateSwap(boltz.CreateSwapRequest{From: boltz.CurrencyBtc, To: boltz.CurrencyBtc, RefundPublicKey: publicKey, Invoice: testInvoice(t, submarineHash), PairHash: submarinePair.Hash})
	check(t, err)
	if sub.Id == "" || sub.ExpectedAmount == 0 {
		t.Fatal("incomplete submarine response")
	}
	subTree := verifiedTree(t, sub.SwapTree, sub.ClaimPublicKey, boltz.NormalSwap, sub.TimeoutBlockHeight, submarineHash[:], sub.Address)
	refundOutput := spendOutput(t, subTree, sub.Address, boltz.NormalSwap, nil, sub.TimeoutBlockHeight)
	verifySpend(t, refundOutput, constructSpend(t, refundOutput, nil))

	reverseRequest := boltz.CreateReverseSwapRequest{From: boltz.CurrencyBtc, To: boltz.CurrencyBtc, ClaimPublicKey: publicKey, PreimageHash: hash[:], InvoiceAmount: 100_000, PairHash: reversePair.Hash}
	reverse, err := api.CreateReverseSwap(reverseRequest)
	check(t, err)
	if reverse.Id == "" || reverse.OnchainAmount == 0 {
		t.Fatal("incomplete reverse response")
	}
	repeated, err := api.CreateReverseSwap(reverseRequest)
	check(t, err)
	if repeated.Id != reverse.Id {
		t.Fatal("identical reverse request created another swap")
	}
	reverseTree := verifiedTree(t, reverse.SwapTree, reverse.RefundPublicKey, boltz.ReverseSwap, reverse.TimeoutBlockHeight, hash[:], reverse.LockupAddress)
	invoice, err := zpay32.Decode(reverse.Invoice, boltz.Regtest.Btc)
	check(t, err)
	if invoice.PaymentHash == nil || *invoice.PaymentHash != hash {
		t.Fatal("reverse invoice hash mismatch")
	}
	if invoice.MilliSat == nil || *invoice.MilliSat != 100_000_000 {
		t.Fatal("reverse invoice amount mismatch")
	}
	claimOutput := spendOutput(t, reverseTree, reverse.LockupAddress, boltz.ReverseSwap, preimage, reverse.TimeoutBlockHeight)
	claimOutput.SwapId = reverse.Id
	claimOutput.Cooperative = true
	claim := constructSpend(t, claimOutput, api)
	if transport.claimCalls.Load() != 1 {
		t.Fatal("official SDK did not attempt cooperative claim")
	}
	if len(claim.MsgTx().TxIn[0].Witness) != 4 {
		t.Fatal("official SDK did not fall back to script path")
	}
	verifySpend(t, claimOutput, claim)
	claimHex, err := claim.Serialize()
	check(t, err)
	broadcastID, err := api.BroadcastTransaction(boltz.CurrencyBtc, claimHex)
	check(t, err)
	if broadcastID != claim.Hash() {
		t.Fatal("broadcast response transaction id mismatch")
	}

	ws := api.NewWebsocket()
	check(t, ws.Connect())
	defer ws.Close()
	check(t, ws.Subscribe([]string{sub.Id, reverse.Id}))
	seen := map[string]bool{}
	deadline := time.After(5 * time.Second)
	for len(seen) < 2 {
		select {
		case update := <-ws.Updates:
			if update.Id != sub.Id && update.Id != reverse.Id {
				t.Fatal("unexpected swap update")
			}
			status, err := api.SwapStatus(update.Id)
			check(t, err)
			if status.Status != update.Status {
				t.Fatal("REST and WebSocket status disagree")
			}
			seen[update.Id] = true
		case <-deadline:
			t.Fatal("missing initial WebSocket status snapshots")
		}
	}

	_, err = api.CreateReverseSwap(boltz.CreateReverseSwapRequest{From: boltz.CurrencyBtc, To: boltz.CurrencyLiquid, ClaimPublicKey: publicKey, PreimageHash: hash[:], InvoiceAmount: 100_000})
	if err == nil {
		t.Fatal("unsupported asset was accepted")
	}
	_, err = api.CreateChainSwap(boltz.ChainRequest{From: boltz.CurrencyBtc, To: boltz.CurrencyBtc, PreimageHash: hash[:], ClaimPublicKey: publicKey, RefundPublicKey: publicKey, UserLockAmount: 100_000})
	if err == nil {
		t.Fatal("unsupported chain swap was accepted")
	}
}

// The SDK chooses its own fallback after a genuine HTTP error response.
func TestOfficialReverseClaimFallback(t *testing.T) {
	var fixture struct {
		Cases map[string]vector `json:"cases"`
	}
	check(t, json.Unmarshal(vectorJSON, &fixture))
	v := fixture.Cases["reverse"]
	tree := verifiedTree(t, &v.SwapTree, unhex(t, v.ProviderPublicKey), boltz.ReverseSwap, v.TimeoutBlockHeight, unhex(t, v.PreimageHash), v.Address)
	var attempts atomic.Int32
	var preimageSent atomic.Bool
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost || r.URL.Path != "/v2/swap/reverse/fixture/claim" {
			t.Errorf("unexpected API call: %s %s", r.Method, r.URL.Path)
		}
		attempts.Add(1)
		var request boltz.ClaimRequest
		if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
			t.Error(err)
		}
		preimageSent.Store(bytes.Equal(request.Preimage, unhex(t, v.Preimage)))
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusNotImplemented)
		_, _ = w.Write([]byte(`{"error":"cooperative signing is not supported"}`))
	}))
	defer server.Close()
	api := &boltz.Api{URL: server.URL, Client: http.Client{Timeout: 5 * time.Second}}
	output := spendOutput(t, tree, v.Address, boltz.ReverseSwap, unhex(t, v.Preimage), v.TimeoutBlockHeight)
	output.Cooperative = true
	spend := constructSpend(t, output, api)
	if attempts.Load() != 1 || !preimageSent.Load() {
		t.Fatal("expected exactly one default cooperative attempt containing the preimage")
	}
	if len(spend.MsgTx().TxIn[0].Witness) != 4 {
		t.Fatal("expected automatic script-path fallback")
	}
	verifySpend(t, output, spend)
}
