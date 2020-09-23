import pandas as pd


def clean(df: pd.DataFrame) -> pd.DataFrame:
    return df[df['Not_Used'] == 0]


def parse_floor(x: str) -> int:
    floor = x.split('/')[0]
    return int(floor) if floor.isdigit() else 0


def parse_square(x: str) -> float:
    return float(x.replace(' кв.м', '')) if x else None


def handle_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    # Change columns type
    df['Date_Add'] = df['Date_Add'].apply(lambda x: pd.to_datetime(x, format='%d.%m.%Y %H:%M:%S'))
    df['Date_Expiration'] = df['Date_Expiration'].apply(lambda x: pd.to_datetime(x, format='%d.%m.%Y'))
    df['Year_Building'] = df['Year_Building'].astype(float)
    df['Rooms_Number'] = df['Rooms_Number'].astype(int)
    df['Floors_In_Building'] = df['Floors_In_Building'].astype(int)

    # Change columns values
    df['Square_Total'] = df['Square_Total'].apply(parse_square)
    df['Square_Kitchen'] = df['Square_Kitchen'].apply(parse_square)
    df['Square_Living'] = df['Square_Living'].apply(parse_square)
    df['Address'] = 'Томск, ' + df['Address']
    df['Price'] = df['Price'] / 1000

    # Insert new columns
    df.insert(7, 'Floor', df['Floor_Numbers_Of_Floors'].apply(parse_floor))
    df.insert(19, 'Not_Used', 0)
    df.insert(20, 'Not_Used_Description', None)

    # Filter dataset
    df = df[df['Sales_Type'] == 'вторичное']

    # Drop useless columns
    df.drop(['Floor_Numbers_Of_Floors', 'Id', 'Sales_Type'], axis=1, inplace=True)

    return df


def filter_df_main(df: pd.DataFrame) -> pd.DataFrame:
    df.drop(['Square_Kitchen', 'Square_Living', 'Apartment_Type'], axis=1, inplace=True)
    fill_na_list = ['Material', 'Apartment_Condition', 'Bathroom_Type', 'Balcony_Loggia']
    df.loc[:, fill_na_list] = df.loc[:, fill_na_list].fillna('Нет значения')
    df = df[df['Rooms_Number'] <= 4]
    df = df[[i in ['кирпич', 'панель', 'монолит', 'Нет значения'] for i in df['Material']]]
    df = df[df['Year_Building'] >= 1960]
    df = df[df['Floors_In_Building'] != 1]
    df = df[df['Floor'] != 0]

    return df


def filter_df_room_1(df: pd.DataFrame) -> pd.DataFrame:
    # Price
    idx_lower_price = df['Price'] < 500
    idx_higher_price = df['Price'] > 3000
    df.loc[idx_lower_price, 'Not_Used'] = 1
    df.loc[idx_higher_price, 'Not_Used'] = 1

    # Square_Total
    idx_lower_square = df['Square_Total'] < 12
    idx_higher_square = df['Square_Total'] > 50
    df.loc[idx_lower_square, 'Not_Used'] = 1
    df.loc[idx_higher_square, 'Not_Used'] = 1

    # Floors_In_Building
    # Floors_In_Building == 3
    idx_higher_price_3_floor = (df['Floors_In_Building'] == 3) & (df['Price'] > 2000)
    df.loc[idx_higher_price_3_floor, 'Not_Used'] = 1

    # Floors_In_Building == 4
    idx_higher_price_4_floor = (df['Floors_In_Building'] == 4) & (df['Price'] > 2500)
    df.loc[idx_higher_price_4_floor, 'Not_Used'] = 1

    # Floors_In_Building == 12
    idx_higher_price_12_floor = (df['Floors_In_Building'] == 12) & (df['Price'] > 2800)
    df.loc[idx_higher_price_12_floor, 'Not_Used'] = 1

    # Floors_In_Building == 19
    idx_lower_price_19_floor = (df['Floors_In_Building'] == 19) & (df['Price'] < 1500)
    df.loc[idx_lower_price_19_floor, 'Not_Used'] = 1

    # Apartment_Condition
    idx_lower_price_apartment_condition = (df['Apartment_Condition'] == 'черновая отделка') & (df['Price'] < 1400)
    idx_higher_price_apartment_condition = (df['Apartment_Condition'] == 'черновая отделка') & (df['Price'] > 2500)
    df.loc[idx_lower_price_apartment_condition, 'Not_Used'] = 1
    df.loc[idx_higher_price_apartment_condition, 'Not_Used'] = 1

    # Balcony_Loggia
    balcony_loggia_to_delete = ['2 лоджии',
                                'балкон и лоджия, остекление',
                                '2 балкона',
                                '2 лоджии, остекление',
                                '2 балкона, остекление',
                                'балкон и лоджия']
    idx_to_delete = [i in balcony_loggia_to_delete for i in df['Balcony_Loggia']]
    df.loc[idx_to_delete, 'Not_Used'] = 1

    return clean(df)


def convert_to_dummies(df: pd.DataFrame) -> pd.DataFrame:
    df['District_Kir'] = df['District'] == 'кировский район'
    df['District_Len'] = df['District'] == 'ленинский район'
    df['District_Sov'] = df['District'] == 'советский район'
    df['District_Oct'] = df['District'] == 'октябрьский район'

    df['Material_Brick'] = df['Material'] == 'кирпич'
    df['Material_Monolith'] = df['Material'] == 'монолит'
    df['Material_Panel'] = df['Material'] == 'панель'
    df['Material_Not_Info'] = df['Material'] == 'Нет значения'

    df['Apartment_Condition_Excellent'] = df['Apartment_Condition'] == 'в отличном состоянии'
    df['Apartment_Condition_Good'] = df['Apartment_Condition'] == 'в хорошем состоянии'
    df['Apartment_Condition_Need_Repair'] = df['Apartment_Condition'] == 'требуется ремонт'
    df['Apartment_Condition_Raw_Finish'] = df['Apartment_Condition'] == 'черновая отделка'
    df['Apartment_Condition_Not_Info'] = df['Apartment_Condition'] == 'Нет значения'

    df['Bathroom_Type_Combined'] = df['Bathroom_Type'] == 'совмещенный'
    df['Bathroom_Type_Separate'] = df['Bathroom_Type'] == 'раздельный'
    df['Bathroom_Type_Not_Info'] = df['Bathroom_Type'] == 'Нет значения'

    df['Balcony_Loggia_Balcony'] = df['Balcony_Loggia'] == 'балкон'
    df['Balcony_Loggia_Loggia'] = df['Balcony_Loggia'] == 'лоджия'
    df['Balcony_Loggia_Balcony_Glazing'] = df['Balcony_Loggia'] == 'балкон, остекление'
    df['Balcony_Loggia_Loggia_Glazing'] = df['Balcony_Loggia'] == 'лоджия, остекление'
    df['Balcony_Loggia_Not_Info'] = df['Balcony_Loggia'] == 'Нет значения'
    df.drop(['District', 'Material', 'Apartment_Condition', 'Bathroom_Type', 'Balcony_Loggia'], axis=1, inplace=True)

    return df
