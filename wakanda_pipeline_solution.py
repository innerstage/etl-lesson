import pandas as pd
import calendar

from bamboo_lib.helpers import grab_connector
from bamboo_lib.logger import logger
from bamboo_lib.models import EasyPipeline, PipelineStep, Parameter
from bamboo_lib.steps import LoadStep


class OpenStep(PipelineStep):
    def run_step(self, prev, params):
        filename = "Wakanda_{}_{}.csv".format(params.get("trade_flow"), params.get("year"))
        logger.info("Opening {}...".format(filename))
        df = pd.read_csv("data/" + filename)
        
        return df


class TransformStep(PipelineStep):
    def run_step(self, prev, params):
        filename = "Wakanda_{}_{}.csv".format(params.get("trade_flow"), params.get("year"))
        logger.info("Transforming {}...".format(filename))
        df = prev

        # 1. Change order of columns
        cols = list(df.columns)
        cols = [cols[-1]] + cols[:-1]
        df = df[cols]
        
        # 2. Melt Months
        month_list = [m.upper() for m in calendar.month_name[1:]]
        df = df.melt(id_vars=["ORIGIN_OR_DESTINATION"], value_vars=month_list, var_name="month", value_name="total")

        # 3. Change column names
        df = df.rename(columns={"ORIGIN_OR_DESTINATION": "country"})

        # 4. Drop NaN values
        df = df.dropna()

        # 5. Map country names to ISO3
        country_df = pd.read_csv("resources/country_iso3_codes.csv")
        country_map = {k:v for (k,v) in zip(country_df["country_name"], country_df["country_iso3"])}
        df["country"] = df["country"].map(country_map).str.lower()

        # 6. Map month names to numeric
        month_list = [m.upper() for m in calendar.month_name]
        month_map = {month_list[i]: i for i in range(1,13)}
        df["month"] = df["month"].map(month_map)

        # 7. Create trade flow column
        flow_map = {"IMP": 1, "EXP": 2}
        df["trade_flow"] = flow_map[params["trade_flow"]]

        # 8. Create year column and time_id column
        df["year"] = params["year"]
        df["time_id"] = (df["year"] + df["month"].astype(str).str.zfill(2)).astype(int)
        df = df[["time_id", "country", "trade_flow", "total"]]

        # 9. Print the DataFrame
        print(df.head())
        print(df.isnull().any())

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
    for f in ["EXP", "IMP"]:
        for y in range(2020, 2026):
            wakanda_pipeline.run(
                {
                    "output-db": "postgres-remote",
                    "trade_flow": f,
                    "year": str(y)
                }
            )


# BAMBOO-CLI COMMAND
# bamboo-cli --folder . --entry wakanda_pipeline_solution --output-db="postgres-remote" --trade_flow="EXP" --year="2020"