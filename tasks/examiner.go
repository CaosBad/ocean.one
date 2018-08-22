package main

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"time"

	"cloud.google.com/go/spanner"
	bot "github.com/MixinNetwork/bot-api-go-client"
	number "github.com/MixinNetwork/go-number"
	"github.com/MixinNetwork/ocean.one/config"
	"github.com/MixinNetwork/ocean.one/engine"
	"github.com/MixinNetwork/ocean.one/persistence"
	uuid "github.com/satori/go.uuid"
	"github.com/ugorji/go/codec"
	"google.golang.org/api/iterator"
)

const (
	AmountPrecision = 4
	MaxPrice        = 1000000000
	MaxAmount       = 5000000000
	MaxFunds        = MaxPrice * MaxAmount

	MixinAssetId   = "c94ac88f-4671-3976-b60a-09064f1811e8"
	BitcoinAssetId = "c6d0c728-2624-429b-8e0d-d9d19b6592fa"
	USDTAssetId    = "815b0b1a-2764-3736-8faa-42d694fa620a"

	PollInterval = 100 * time.Millisecond
)

func main() {
	checkpoint := time.Now().Add(-1 * time.Hour)
	scanningSnapshot(setupContext(), checkpoint)
}

func setupContext() context.Context {
	client, _ := spanner.NewClientWithConfig(context.Background(), config.GoogleCloudSpanner, spanner.ClientConfig{NumChannels: 4,
		SessionPoolConfig: spanner.SessionPoolConfig{
			HealthCheckInterval: 5 * time.Second,
		},
	})
	return persistence.SetupSpanner(context.Background(), client)
}

func scanningSnapshot(ctx context.Context, checkpoint time.Time) {
	if checkpoint.IsZero() {
		checkpoint = time.Now().UTC()
	}
	const limit = 500
	for {
		log.Println(checkpoint.Format(time.RFC3339Nano))
		snapshots, err := requestMixinNetwork(ctx, checkpoint, limit)
		if err != nil {
			log.Println("PollMixinNetwork ERROR", err)
			time.Sleep(PollInterval)
			continue
		}
		for _, s := range snapshots {
			for i := 0; i < 3; i++ {
				if err := processSnapshot(ctx, s); err == nil {
					break
				}
				time.Sleep(100 * time.Millisecond)
			}
			checkpoint = s.CreatedAt
		}
	}
}

func processSnapshot(ctx context.Context, s *Snapshot) error {
	if s.OpponentId == "" || s.TraceId == "" {
		return nil
	}
	if number.FromString(s.Amount).Exhausted() {
		return nil
	}

	action, err := decryptOrderAction(ctx, s.Data)
	if err != nil {
		return nil
	}
	if len(action.U) > 16 {
		return nil
	}
	if action.O.String() != uuid.Nil.String() {
		return nil
	}

	if action.A.String() == s.Asset.AssetId {
		return nil
	}
	if action.T != engine.OrderTypeLimit && action.T != engine.OrderTypeMarket {
		return nil
	}

	quote, _ := getQuoteBasePair(s, action)
	if quote == "" {
		return nil
	}

	priceDecimal := number.FromString(action.P)
	maxPrice := number.NewDecimal(MaxPrice, int32(QuotePrecision(quote)))
	if priceDecimal.Cmp(maxPrice) > 0 {
		return nil
	}
	price := priceDecimal.Integer(QuotePrecision(quote))
	if action.T == engine.OrderTypeLimit {
		if price.IsZero() {
			return nil
		}
	} else if !price.IsZero() {
		return nil
	}

	fundsPrecision := AmountPrecision + QuotePrecision(quote)
	funds := number.NewInteger(0, fundsPrecision)
	amount := number.NewInteger(0, AmountPrecision)

	assetDecimal := number.FromString(s.Amount)
	if action.S == engine.PageSideBid {
		maxFunds := number.NewDecimal(MaxFunds, int32(fundsPrecision))
		if assetDecimal.Cmp(maxFunds) > 0 {
			return nil
		}
		funds = assetDecimal.Integer(fundsPrecision)
		if funds.Decimal().Cmp(QuoteMinimum(quote)) < 0 {
			return nil
		}
	} else {
		maxAmount := number.NewDecimal(MaxAmount, AmountPrecision)
		if assetDecimal.Cmp(maxAmount) > 0 {
			return nil
		}
		amount = assetDecimal.Integer(AmountPrecision)
		if action.T == engine.OrderTypeLimit && price.Mul(amount).Decimal().Cmp(QuoteMinimum(quote)) < 0 {
			return nil
		}
	}
	order, err := readOder(ctx, s.TraceId)
	if err != nil {
		log.Println(fmt.Sprintf("OrderId %s, ERR %s", s.TraceId, err))
	}
	if order == nil {
		log.Println(fmt.Sprintf("Missing OrderId %s", s.TraceId))
	}
	return nil
}

func readOder(ctx context.Context, orderId string) (*persistence.Order, error) {
	it := persistence.Spanner(ctx).Single().Read(ctx, "orders", spanner.Key{orderId},
		[]string{"order_id", "order_type", "quote_asset_id", "base_asset_id", "state", "user_id"})
	defer it.Stop()

	row, err := it.Next()
	if err == iterator.Done {
		return nil, nil
	} else if err != nil {
		return nil, err
	}
	var o persistence.Order
	err = row.Columns(&o.OrderId, &o.OrderType, &o.QuoteAssetId, &o.BaseAssetId, &o.State, &o.UserId)
	if err != nil {
		return nil, err
	}
	return &o, nil
}

func decryptOrderAction(ctx context.Context, data string) (*OrderAction, error) {
	payload, err := base64.StdEncoding.DecodeString(data)
	if err != nil {
		payload, err = base64.URLEncoding.DecodeString(data)
		if err != nil {
			return nil, err
		}
	}
	var action OrderAction
	decoder := codec.NewDecoderBytes(payload, new(codec.MsgpackHandle))
	err = decoder.Decode(&action)
	if err != nil {
		return nil, err
	}
	switch action.T {
	case "L":
		action.T = engine.OrderTypeLimit
	case "M":
		action.T = engine.OrderTypeMarket
	}
	switch action.S {
	case "A":
		action.S = engine.PageSideAsk
	case "B":
		action.S = engine.PageSideBid
	}
	return &action, nil
}

func getQuoteBasePair(s *Snapshot, a *OrderAction) (string, string) {
	var quote, base string
	if a.S == engine.PageSideAsk {
		quote, base = a.A.String(), s.Asset.AssetId
	} else if a.S == engine.PageSideBid {
		quote, base = s.Asset.AssetId, a.A.String()
	} else {
		return "", ""
	}
	if quote == base {
		return "", ""
	}
	if quote != BitcoinAssetId && quote != USDTAssetId && quote != MixinAssetId {
		return "", ""
	}
	if quote == BitcoinAssetId && base == USDTAssetId {
		return "", ""
	}
	if quote == MixinAssetId && base == USDTAssetId {
		return "", ""
	}
	if quote == MixinAssetId && base == BitcoinAssetId {
		return "", ""
	}
	return quote, base
}

func QuotePrecision(assetId string) uint8 {
	switch assetId {
	case MixinAssetId:
		return 8
	case BitcoinAssetId:
		return 8
	case USDTAssetId:
		return 4
	default:
		log.Panicln("QuotePrecision", assetId)
	}
	return 0
}

func QuoteMinimum(assetId string) number.Decimal {
	switch assetId {
	case MixinAssetId:
		return number.FromString("0.0001")
	case BitcoinAssetId:
		return number.FromString("0.0001")
	case USDTAssetId:
		return number.FromString("1")
	default:
		log.Panicln("QuoteMinimum", assetId)
	}
	return number.Zero()
}

func requestMixinNetwork(ctx context.Context, checkpoint time.Time, limit int) ([]*Snapshot, error) {
	uri := fmt.Sprintf("/network/snapshots?offset=%s&order=ASC&limit=%d", checkpoint.Format(time.RFC3339Nano), limit)
	token, err := bot.SignAuthenticationToken(config.ClientId, config.SessionId, config.SessionKey, "GET", uri, "")
	if err != nil {
		return nil, err
	}
	body, err := bot.Request(ctx, "GET", uri, nil, token)
	if err != nil {
		return nil, err
	}
	var resp struct {
		Data  []*Snapshot `json:"data"`
		Error string      `json:"error"`
	}
	err = json.Unmarshal(body, &resp)
	if err != nil {
		return nil, err
	}
	if resp.Error != "" {
		return nil, errors.New(resp.Error)
	}
	return resp.Data, nil
}

type Error struct {
	Status      int    `json:"status"`
	Code        int    `json:"code"`
	Description string `json:"description"`
}

type Snapshot struct {
	SnapshotId string `json:"snapshot_id"`
	Amount     string `json:"amount"`
	Asset      struct {
		AssetId string `json:"asset_id"`
	} `json:"asset"`
	CreatedAt time.Time `json:"created_at"`

	TraceId    string `json:"trace_id"`
	UserId     string `json:"user_id"`
	OpponentId string `json:"opponent_id"`
	Data       string `json:"data"`
}

type OrderAction struct {
	U []byte    // user
	S string    // side
	A uuid.UUID // asset
	P string    // price
	T string    // type
	O uuid.UUID // order
}
