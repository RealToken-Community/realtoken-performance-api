# RealToken Performance API

## Table of Contents

- [Overview](#overview)
- [Performance Indicators](#performance-indicators)
  - [Realized Gain](#1-realized-gain)
  - [Unrealized Gain](#2-unrealized-gain)
  - [Distributed Income](#3-distributed-income)
  - [Overall Performance](#4-overall-performance)
- [Wallet resolution](#wallet-resolution)
- [API Endpoints](#api-endpoints)
  - [Health Check](#health-check)
  - [Realtoken Performance](#realtoken-performance)
- [Events included in performance calculations](#events-included-in-performance-calculations)

------------------------------------------------------------------------ 

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
through a sale, a detokenisation or being liquidated).

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

-   the **current Realt value of held tokens**
-   the **average acquisition price of those tokens**

Conceptually:

    Unrealized Gain = Current Realt Value - Cost Basis of Held Tokens

This metric reflects the **potential gain if the position were sold at
the current Realt price**.

------------------------------------------------------------------------

### 3. Distributed Income

Distributed income corresponds to **income paid by Realtoken**.

The API aggregates all distributions received across time.

Conceptually:

    Distributed Income = Sum of all payments received

The total distributed income is calculated by aggregating all payments listed in the CSV income files provided by RealT.

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

Only **one wallet address** is required.

The API automatically retrieves **all wallets linked to the same RealT user ID** and computes performance across the entire set.

This is intentional: due to **internal transfers between wallets**, calculating performance on a single wallet can be misleading (e.g. tokens received without a purchase).  
For consistency, performance is always computed **at the user level (all linked wallets)**.

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

### Realtoken Performance

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

The performance calculator builds a **normalized event history** from several data sources.

The following **event types** are included in the calculation:

| Event type | Description |
|------------|-------------|
| **Purchases from RealT** (gnosis) | Direct purchases of Realtokens from the RealT platform. |
| **YAM** (v1) (gnosis)| Buys and sells of Realtokens via the YAM smart contract |
| **SwapCat** (gnosis)| Buys and sells of Realtokens via the SwapCat smart contract |
| **RMM liquidations** (v3) | When the user **receives** tokens from an RMM v3 liquidation, this is an IN position. When the user’s position is **liquidated** (tokens taken), this is an OUT disposals. |
| **Detokenizations** (gnosis) | Redemption of Realtokens for the underlying asset. |
| **Distributed income** | Aggregation of the CSV income files provided by RealT. |

The API response includes an **`event_types`** array listing all event type labels, and an **`events`** object containing the full list of normalized events per token that were used to compute the performance. 

### Event consolidation (handling missing data)

In some cases, the available event history is **incomplete** (e.g. missing buy or sell events).  
This can lead to inconsistencies between the reconstructed balance and the **actual wallet balance at a given time**.

To address this, the API performs a **virtual consolidation**:

- Missing quantities are adjusted by injecting **virtual events**
- These events are **not included in the event history output**
- They are used **only internally for performance calculations**

These virtual events assume a **purchase at the RealT listing price** (initial price when the token was released on the platform).


### Event types not yet implemented (to be added)

- [ ] **YAM** (v1) (ethereum)
- [ ] **RMM Liquidations** (v2)
- [ ] **Detokenizations** (ethereum)
- [ ] **Purchases from RealT** (ethereum)  
- [ ] **Limited sale purchases from Realt** (gnosis & ethereum)
- [ ] **LevinSwap LP positions** (balances)
- [ ] **LevinSwap LP PnL** (pool rebalancing / impermanent loss / swaps)


