import json
import Connectors
import Transformations
import AutoML
try:
    PredictHighestIncome_DBFS = Connectors.DBFSConnector.fetch(
        [], {}, "5e8ee936a8429fe1b520ce64", spark, "{'url': '/Demo/PredictHighestIncomeTrain.csv', 'file_type': 'Delimeted', 'dbfs_token': 'dapib3c8e0614707f7e6d2addea6ce7c33d0', 'dbfs_domain': 'westus.azuredatabricks.net', 'delimiter': ',', 'is_header': 'Use Header Line'}")

except Exception as ex:
    print(ex)
try:
    PredictHighestIncome_AutoFE = Transformations.TransformationMain.run(["5e8ee936a8429fe1b520ce64"], {"5e8ee936a8429fe1b520ce64": PredictHighestIncome_DBFS}, "5e8ee936a8429fe1b520ce65", spark, json.dumps({"FE": [{"transformationsData": {"feature_label": "Occupation"}, "feature": "Occupation", "type": "string", "selected": "True", "replaceby": "max", "stats": {"count": "470", "mean": "", "stddev": "", "min": "AGRICULTURAL", "max": "Writers and authors", "missing": "0"}, "transformation": "String Indexer"}, {"transformationsData": {}, "feature": "M_workers", "transformation": "", "type": "numeric", "replaceby": "mean", "selected": "True", "stats": {"count": "470", "mean": "328.09", "stddev": "2850.46", "min": "0", "max": "60746", "missing": "0"}}, {"transformationsData": {"feature_label": "M_weekly"}, "feature": "M_weekly", "type": "string", "selected": "True", "replaceby": "max", "stats": {"count": "470", "mean": "996.51", "stddev": "387.77", "min": "1001", "max": "Na", "missing": "0"}, "transformation": "String Indexer"}, {"transformationsData": {}, "feature": "F_workers", "transformation": "", "type": "numeric", "replaceby": "mean", "selected": "True", "stats": {"count": "470", "mean": "274.03", "stddev": "2313.77", "min": "0", "max": "48334", "missing": "0"}}, {"transformationsData": {"feature_label": "F_weekly"}, "feature": "F_weekly", "type": "string", "selected": "True", "replaceby": "max", "stats": {"count": "470", "mean": "789.35", "stddev": "283.43", "min": "1004", "max": "Na", "missing": "0"}, "transformation": "String Indexer"}, {
                                                                         "transformationsData": {}, "feature": "All_workers", "transformation": "", "type": "numeric", "replaceby": "mean", "selected": "True", "stats": {"count": "470", "mean": "602.16", "stddev": "5126.85", "min": "0", "max": "109080", "missing": "0"}}, {"transformationsData": {"feature_label": "All_weekly"}, "feature": "All_weekly", "type": "string", "selected": "True", "replaceby": "max", "stats": {"count": "470", "mean": "903.19", "stddev": "343.87", "min": "1000", "max": "Na", "missing": "0"}, "transformation": "String Indexer"}, {"feature": "Occupation_transform", "transformation": "", "transformationsData": {}, "type": "real", "selected": "True", "stats": {"count": "470", "mean": "234.5", "stddev": "135.82", "min": "0.0", "max": "469.0", "missing": "0"}}, {"feature": "M_weekly_transform", "transformation": "", "transformationsData": {}, "type": "real", "selected": "True", "stats": {"count": "470", "mean": "37.66", "stddev": "57.15", "min": "0.0", "max": "187.0", "missing": "0"}}, {"feature": "F_weekly_transform", "transformation": "", "transformationsData": {}, "type": "real", "selected": "True", "stats": {"count": "470", "mean": "26.77", "stddev": "45.96", "min": "0.0", "max": "158.0", "missing": "0"}}, {"feature": "All_weekly_transform", "transformation": "", "transformationsData": {}, "type": "real", "selected": "True", "stats": {"count": "470", "mean": "63.19", "stddev": "77.68", "min": "0.0", "max": "241.0", "missing": "0"}}]}))

except Exception as ex:
    print(ex)
try:
    AutoML.functionRegression(PredictHighestIncome_AutoFE, [
                              "Occupation", "M_workers", "M_weekly", "F_workers", "F_weekly", "All_workers"], "All_weekly")

except Exception as ex:
    print(ex)
