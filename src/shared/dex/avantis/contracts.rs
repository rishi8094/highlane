use alloy::{primitives::Address, sol};

sol! {
    #[derive(Debug)]
    struct Trade {
        address trader;
        uint256 pairIndex;
        uint256 index;
        uint256 initialPosToken;
        uint256 positionSizeUSDC;
        uint256 openPrice;
        bool buy;
        uint256 leverage;
        uint256 tp;
        uint256 sl;
        uint256 timestamp;
    }

    #[derive(Debug)]
    #[sol(rpc)]
    interface ICallbacks {
        event MarketExecuted(
            uint256 orderId,
            Trade t,
            bool open,
            uint256 price,
            uint256 positionSizeUSDC,
            int256 percentProfit,
            uint256 usdcSentToTrader,
            bool isPnl
        );

        event LimitExecuted(
            uint256 orderId,
            uint256 limitIndex,
            Trade t,
            uint8 orderType,
            uint256 price,
            uint256 positionSizeUSDC,
            int256 percentProfit,
            uint256 usdcSentToTrader,
            bool isPnl
        );
    }

    #[sol(rpc)]
    interface IPairStorage {
        function getPairData(uint256 _pairIndex) external view returns (
            string memory from,
            string memory to,
            uint256 numTiers,
            uint256[] memory tierThresholds,
            uint256[] memory timer
        );
        function pairsCount() external view returns (uint256);
    }

    #[sol(rpc)]
    interface ITradingStorage {
        function callbacks() external view returns (address);
        function trading() external view returns (address);
    }

    #[sol(rpc)]
    interface ITrading {
        function openTrade(Trade t, uint8 _type, uint256 _slippageP) external;
        function closeTradeMarket(uint256 _pairIndex, uint256 _index, uint256 _amount) external;
        function cancelOpenLimitOrder(uint256 _pairIndex, uint256 _index) external;
        function cancelPendingMarketOrder(uint256 _id) external;
        function updateOpenLimitOrder(
            uint256 _pairIndex,
            uint256 _index,
            uint256 _price,
            uint256 _slippageP,
            uint256 _tp,
            uint256 _sl
        ) external;
        function updateTpAndSl(
            uint256 _pairIndex,
            uint256 _index,
            uint256 _newSl,
            uint256 _newTP,
            bytes[] priceUpdateData,
            uint8 _priceSourcing
        ) external;
        function updateSl(
            uint256 _pairIndex,
            uint256 _index,
            uint256 _newSl,
            bytes[] priceUpdateData,
            uint8 _priceSourcing
        ) external;
        function updateMargin(
            uint256 _pairIndex,
            uint256 _index,
            uint8 _type,
            uint256 _amount,
            bytes[] priceUpdateData,
            uint8 _priceSourcing
        ) external;
    }

    #[sol(rpc)]
    interface IBatchCall {
        struct Call3 {
            address target;
            bool allowFailure;
            bytes callData;
        }
        struct Result3 {
            bool success;
            bytes returnData;
        }
        function aggregate3(Call3[] calldata calls) external payable returns (Result3[] memory returnData);
    }
}

pub const TRADING_STORAGE: &str = "0x8a311D7048c35985aa31C131B9A13e03a5f7422d";
pub const PAIR_STORAGE: &str = "0x5db3772136e5557EFE028Db05EE95C84D76faEC4";
pub const MULTICALL3: &str = "0xcA11bde05977b3631167028862bE2a173976CA11";

pub fn parse_addr(s: &str) -> Address {
    s.parse().expect("invalid address")
}
