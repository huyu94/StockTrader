from core.pipelines.realtime_kline_pipeline import RealtimeKlinePipeline


pipeline = RealtimeKlinePipeline()


pipeline.run(ts_codes=["000001.SZ", "600000.SH"])