# RealToken Performance API

## Overview

The **RealToken Performance API** calculates and exposes financial
performance metrics for portfolios holding Realtoken assets.

The API reconstructs the complete financial history of a wallet by
combining multiple data sources such as blockchain events, RealT
purchases, YAM marketplace activity, and income distributions.

From these inputs, the API produces four main performance indicators:

1.  **Realized Gain**
2.  **Unrealized Gain**
3.  **Distributed Income**
4.  **Overall Performance**

These indicators are available:

-   **Per token**
-   **For the entire portfolio**

The goal of this API is to provide a **clear financial performance
view** for Realtoken investors.

------------------------------------------------------------------------

## Performance Indicators

### 1. Realized Gain

Realized gain represents the profit or loss generated from **completed
sales of tokens**.

A realized gain occurs when tokens leave the portfolio (for example
through a sale or liquidation).

The gain is calculated using the **Weighted Average Cost (WAC)** method:

-   Each outgoing token movement is matched with an average acquisition
    price.
-   The difference between the sale value and the acquisition value
    represents the realized gain.

Formula conceptually:

    Realized Gain = Sale Value - Cost Basis of Sold Tokens

This metric reflects **profit already locked in by closing positions**.

------------------------------------------------------------------------

### 2. Unrealized Gain

Unrealized gain represents the **current profit or loss on tokens that
are still held**.

It is calculated by comparing:

-   the **current market value of held tokens**
-   the **average acquisition price of those tokens**

Conceptually:

    Unrealized Gain = Current Value - Cost Basis of Held Tokens

This metric reflects the **potential gain if the position were sold at
the current price**.

------------------------------------------------------------------------

### 3. Distributed Income

Distributed income corresponds to **rental income paid by RealToken
properties**.

RealToken holders periodically receive rental distributions based on the
number of tokens they own.

The API aggregates all distributions received across time.

Conceptually:

    Distributed Income = Sum of all rental payments received

This component is a **core part of the RealToken investment return**.

------------------------------------------------------------------------

### 4. Overall Performance

Overall performance combines the three previous indicators into a single
global performance metric.

    Overall Return = Realized Gain + Unrealized Gain + Distributed Income

From this value, the API computes a **Return on Investment (ROI)** using
the total acquisition cost.

    ROI = Overall Return / Total Acquisition Cost

This provides a **complete financial view of the investment
performance**.

------------------------------------------------------------------------

## Wallet resolution

Only **one wallet address** needs to be provided.

When the API receives a wallet address, it automatically retrieves **all
wallets associated with the same RealT user ID** and computes the
performance **across the entire set of linked wallets**.

This ensures that the portfolio performance reflects the **full
investment activity of the user**, even if tokens are distributed across
several wallets.

------------------------------------------------------------------------

## API Endpoints

### Health Check

    GET /api/v1/health

Returns the operational status of the API.

``` json
{
"status": "ok",
"utc_datetime": "2026-03-15T21:09:44.692787+00:00"
}
```

------------------------------------------------------------------------

### Portfolio Performance

    GET /api/v1/realtokens-performance

#### Parameters

| Parameter | Type | Description |
|-----------|-------------|-------------|
| wallet    | string | Wallet address to analyze |
| no_cache  | boolean (optional) | If set to `true`, bypasses the cache and forces a fresh performance calculation. |

Example:

    /api/v1/realtokens-performance?wallet=0x123...

This endpoint returns the complete performance dataset including:

-   portfolio performance
-   token-level performance
-   event history

Example response structure:

``` json
{
  "wallets": [
    "0x123...",
    "0x456..."
  ],

  "event_types": [
    "...": "List of all the event type included in the calculator"
  ],

  "events": {
    "...": "Serialized event history grouped by token"
  },

  "performance": {
    "portfolio": {
      "realized": {
        "...": "Realized performance metrics"
      },
      "unrealized": {
        "...": "Unrealized performance metrics"
      },
      "distributed_income": {
        "...": "Distributed income metrics"
      },
      "overall_performance": {
        "...": "Combined overall performance metrics"
      }
    },

    "by_token": {
      "token_uuid": {
        "realized": {},
        "unrealized": {},
        "distributed_income": {},
        "overall_performance": {}
      }
    }
  }
}
```

------------------------------------------------------------------------

## Events included in performance calculations

The performance calculator builds a **normalized event history** from several data sources. Only events that represent **acquisitions (IN)** or **disposals (OUT)** of tokens are used for cost basis and realized PnL.

The following **event types** are included in the calculation:

| Event type | Description |
|------------|-------------|
| **Purchases from RealT** | Direct purchases of Realtokens from the RealT platform. |
| **Transactions from YAM (v1)** | Buys and sells of Realtokens via the YAM protocol |
| **RMM v3 liquidations** | When the user **receives** tokens from an RMM v3 liquidation, this is an IN position. When the user’s position is **liquidated** (tokens taken), this is an OUT disposals. |
| **Detokenizations** | Redemption of Realtokens for the underlying asset. |

The API response includes an **`event_types`** array listing all event type labels, and an **`events`** object containing the full list of normalized events per token that were used to compute the performance. This allows auditors or integrators to verify exactly which on-chain and off-chain data fed into the metrics.
