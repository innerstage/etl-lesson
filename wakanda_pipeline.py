import pandas as pd
import calendar

from bamboo_lib.helpers import grab_connector
from bamboo_lib.logger import logger
from bamboo_lib.models import EasyPipeline, PipelineStep, Parameter
from bamboo_lib.steps import LoadStep


class OpenStep(PipelineStep):
    def run_step(self, prev, params):
        logger.info("OpenStep")

        return df


class TransformStep(PipelineStep):
    def run_step(self, prev, params):
        logger.info("TransformStep")

        df = prev

        return df


class WakandaPipeline(EasyPipeline):
	@staticmethod
	def parameter_list():
		return [
			Parameter("output-db", dtype=str),
			Parameter("trade_flow", dtype=str),
			Parameter("year", dtype=str)
		]

	@staticmethod
	def steps(params):
		db_connector = grab_connector(__file__, params.get("output-db"))

		open_step = OpenStep()
		transform_step = TransformStep()
		load_step = LoadStep(
			table_name="wakanda_trade",
			connector=db_connector,
			if_exists="append",
			pk=["time_id"]
		)

		return [open_step, transform_step, load_step]


if __name__ == "__main__":
    wakanda_pipeline = WakandaPipeline()
    wakanda_pipeline.run(
        {
            "output-db": "",
            "trade_flow": "",
            "year": ""
        }
    )
