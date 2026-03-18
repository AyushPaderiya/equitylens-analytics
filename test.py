from src.transformers.technical_indicators import TechnicalIndicatorsTransformer

t = TechnicalIndicatorsTransformer()
df = t.transform_ticker("AAPL")
print(df.shape)
print(df[["date","close","rsi_14","macd","bb_upper","sma_50","volatility_30d"]].tail(5))
