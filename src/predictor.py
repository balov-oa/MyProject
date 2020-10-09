import logging
import pickle
from pathlib import Path

import pandas as pd

from processor import (
    convert_to_dummies,
    filter_df_main,
    filter_df_room_1,
    handle_dataframe,
)
from sql_connector import get_sqlalchemy_engine

ENGINE = get_sqlalchemy_engine()


def get_model_from_path(path: Path):
    with open(path, 'rb') as file:
        model = pickle.load(file)
    return model


def predict(model, data_to_predict: pd.DataFrame) -> pd.DataFrame:
    return model.predict(data_to_predict).round(2)


def predict_main(data_to_predict: pd.DataFrame) -> pd.DataFrame:
    # Get model info
    sql_expression = f"""SELECT Model_path,
                                Model_features 
                         FROM Models 
                         WHERE Is_main = 1"""
    model_info = ENGINE.execute(sql_expression).fetchone()

    #     if model_info is None:
    #         raise ValueError(f"Model '{model_name}' does not exist")

    model_path = model_info[0]
    model_columns = model_info[1].split('; ')
    difference_between_columns = set(model_columns).difference(set(data_to_predict.columns))

    if difference_between_columns:
        raise ValueError(f'Columns {difference_between_columns} not found in put data')

    # Load model
    model = get_model_from_path(model_path)

    # Make predict
    predictions = predict(model=model, data_to_predict=data_to_predict[model_columns])
    return predictions


def main() -> None:
    query = """SELECT District,
                      Address,
                      Sales_Type,
                      Year_Building,
                      Material,
                      Floor_Numbers_Of_Floors,
                      Floors_In_Building,
                      Apartment_Type,
                      Price,
                      Square_Total,
                      Square_Living,
                      Square_Kitchen,
                      Rooms_Number,
                      Apartment_Condition,
                      Bathroom_Type,
                      Balcony_Loggia,
                      Date_Add,
                      Date_Expiration,
                      Id,
                      Apartment_Key
               FROM Apartment_Tomsk.dbo.Apartments
               WHERE Apartment_Key NOT IN (SELECT Apartment_Key FROM Apartment_Tomsk.dbo.Predictions)
               AND Rooms_Number = 1"""
    data = pd.read_sql_query(query, ENGINE, index_col='Apartment_Key')
    data = handle_dataframe(data)
    data = filter_df_main(data)
    data = filter_df_room_1(data)
    data_to_predict = data.drop(
        ['Date_Add', 'Date_Expiration', 'Address', 'Price', 'Not_Used', 'Not_Used_Description'],
        axis=1
    )
    data_dummies = convert_to_dummies(data_to_predict)
    input_data_message = f'Input data shape: {data_to_predict.shape}'
    print(input_data_message)
    logging.info(input_data_message)
    if not data.empty:
        data['Predict'] = predict_main(data_dummies)
        data['Error'] = (data['Price'] - data['Predict']).round(2)
        data.reset_index(inplace=True)
        data[['Apartment_Key', 'Predict', 'Error']].to_sql('Predictions', ENGINE, if_exists='append', index=False)
    else:
        logging.info('There are no new apartments to predictions')


if __name__ == '__main__':
    log_file = Path(__file__).parent.parent.joinpath('logs').joinpath('predictor.txt')
    logging.basicConfig(
        format='[%(asctime)s] -- %(levelname).3s -- %(message)s',
        datefmt='%Y.%m.%d %H:%M:%S',
        level=logging.DEBUG,
        filename=log_file)

    logging.info('Predictor start')
    try:
        main()
    except Exception as e:
        logging.exception(e)
