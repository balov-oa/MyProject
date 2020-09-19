import pickle
from pathlib import Path

import pandas as pd

from src.processor import handle_dataframe, filter_df_main, filter_df_room_1, convert_to_dummies
from src.sql_connector import get_sqlalchemy_engine

engine = get_sqlalchemy_engine()


def get_model_from_path(path: Path):
    with open(path, 'rb') as file:
        model = pickle.load(file)
    return model


def predict(model, data_to_predict: pd.DataFrame) -> pd.DataFrame:
    raw_predict = model.predict(data_to_predict).round(2)
    predictions = pd.DataFrame({'Apartment_Key': data_to_predict.index,
                                'Predict': raw_predict})
    return predictions


def predict_main(data_to_predict: pd.DataFrame) -> pd.DataFrame:
    # Get model infoq
    sql_expression = f"""SELECT Model_path,
                                Model_features 
                         FROM Models 
                         WHERE Is_main = 1"""
    model_info = engine.execute(sql_expression).fetchone()

    #     if model_info is None:
    #         raise ValueError(f"Model '{model_name}' does not exist")

    model_path = model_info[0]
    model_columns = model_info[1].split('; ')
    difference_between_columns = set(model_columns).difference(set(data_to_predict.columns))

    if difference_between_columns != set():
        raise ValueError(f'Columns {difference_between_columns} not found in put data')

    # Load model
    model = get_model_from_path(model_path)

    # Make predict
    predictions = predict(model=model, data_to_predict=data_to_predict[model_columns])

    return predictions


def main():
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
    data = pd.read_sql_query(query, engine, index_col='Apartment_Key')
    data = handle_dataframe(data)
    data = filter_df_main(data)
    data = filter_df_room_1(data)
    data = data.drop(['Date_Add', 'Date_Expiration', 'Address', 'Price', 'Not_Used', 'Not_Used_Description'], axis=1)
    data_dummies = convert_to_dummies(data)
    print(f'Input data shape: {data.shape}')
    predictions = predict_main(data_dummies)
    predictions.to_sql('Predictions', engine, if_exists='append', index=False)
    return data_dummies, data, predictions
