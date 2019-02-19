import os
import subprocess

import numpy as np
import pytest

import modin.pandas as pd

# Number of rows for synthetic datasets
NUM_ROWS = 100


def teardown_module():
    # Some csv files are created by the tests so we have to make sure we delete
    # them here.
    subprocess.Popen("rm kaggle/*.csv")


@pytest.fixture
def generate_dataset():
    """Generates a synthetic dataset using the given arguments.

    Args:
        columns (list): Column names of the result
        dtypes (list): List of dtypes for the corresponding column
        size (int): Number of rows for result

    Returns:
        Modin dataframe of synthetic data following arguments.
    """
    # Record of files generated for a test
    filenames = []

    def _dataset_builder(filename, columns, dtypes, size=NUM_ROWS):
        def _num_to_str(x):
            letters = ''
            while x:
                mod = (x - 1) % 26
                letters += chr(mod + 65)
                x = (x - 1) // 26
            return ''.join(reversed(letters))

        # Check that the number of column names is the same as the nubmer of dtypes
        if len(columns) != len(dtypes):
            raise ValueError("len(columns) != len(dtypes)")
        result_dict = {}
        for col, dtype in zip(columns, dtypes):
            if dtype is str:
                result_dict[col] = [_num_to_str(x+1) for x in np.arange(size, dtype=int)]
            elif dtype is bool:
                result_dict[col] = [x%2==0 for x in np.arange(size, dtype=int)]
            else:
                result_dict[col] = np.arange(size, dtype=dtype)
        result = pd.DataFrame(result_dict)
        result.to_csv(filename)
        filenames.append(filename)
        return result

    # Return dataset builder factory
    yield _dataset_builder

    # Delete files created
    for filename in filenames:
        if os.path.exists(filename):
            os.remove(filename)

    subprocess.Popen("rm kaggle/*.csv")


def test_kaggle3(generate_dataset):
    pokemon_columns = ['#', 'Name', 'Type 1', 'Type 2', 'HP', 'Attack', 'Defense', 'Sp. Atk', 'Sp. Def', 'Speed', 'Generation', 'Legendary']
    pokemon_dtypes = [int, str, str, str, int, int, int, int, int, int, int, bool]
    generate_dataset("kaggle/pokemon.csv", pokemon_columns, pokemon_dtypes)

    ipynb = subprocess.Popen(["python", "kaggle/kaggle3.py"], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    outs, errs = ipynb.communicate()

    assert ipynb.returncode == 0, errs


def test_kaggle4(generate_dataset):
    columns = ['Id', 'MSSubClass', 'MSZoning', 'LotFrontage', 'LotArea', 'Street',
       'Alley', 'LotShape', 'LandContour', 'Utilities', 'LotConfig',
       'LandSlope', 'Neighborhood', 'Condition1', 'Condition2', 'BldgType',
       'HouseStyle', 'OverallQual', 'OverallCond', 'YearBuilt', 'YearRemodAdd',
       'RoofStyle', 'RoofMatl', 'Exterior1st', 'Exterior2nd', 'MasVnrType',
       'MasVnrArea', 'ExterQual', 'ExterCond', 'Foundation', 'BsmtQual',
       'BsmtCond', 'BsmtExposure', 'BsmtFinType1', 'BsmtFinSF1',
       'BsmtFinType2', 'BsmtFinSF2', 'BsmtUnfSF', 'TotalBsmtSF', 'Heating',
       'HeatingQC', 'CentralAir', 'Electrical', '1stFlrSF', '2ndFlrSF',
       'LowQualFinSF', 'GrLivArea', 'BsmtFullBath', 'BsmtHalfBath', 'FullBath',
       'HalfBath', 'BedroomAbvGr', 'KitchenAbvGr', 'KitchenQual',
       'TotRmsAbvGrd', 'Functional', 'Fireplaces', 'FireplaceQu', 'GarageType',
       'GarageYrBlt', 'GarageFinish', 'GarageCars', 'GarageArea', 'GarageQual',
       'GarageCond', 'PavedDrive', 'WoodDeckSF', 'OpenPorchSF',
       'EnclosedPorch', '3SsnPorch', 'ScreenPorch', 'PoolArea', 'PoolQC',
       'Fence', 'MiscFeature', 'MiscVal', 'MoSold', 'YrSold', 'SaleType',
       'SaleCondition', 'SalePrice']
    dtypes = [int, int, str, float, int, str, float, str, str, str, str, str, str,
        str, str, str, str, int, int, int, int, str, str, str, str, str, float, str,
         str, str, str, str, str, str, int, str, int, int, int, str, str, str, str,
         int, int, int, int, int, int, int, int, int, int, str, int, str, int, float,
         str, float, str, int, int, str, str, str, int, int, int, int, int, int,
         float, float, float, int, int, int, str, str, int]
    generate_dataset("kaggle/train.csv", columns, dtypes)
    generate_dataset("kaggle/test.csv", columns, dtypes)

    ipynb = subprocess.Popen(["python", "kaggle/kaggle4.py"], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    outs, errs = ipynb.communicate()

    assert ipynb.returncode == 0, errs


def test_kaggle5(generate_dataset):
    columns = ['PassengerId', 'Pclass', 'Name', 'Sex', 'Age', 'SibSp', 'Parch',
    'Ticket', 'Fare', 'Cabin', 'Embarked']
    dtypes = [int, int, str, str, float, int, int, str, float, float, str]
    generate_dataset("kaggle/train.csv", columns, dtypes)
    generate_dataset("kaggle/test.csv", columns, dtypes)

    ipynb = subprocess.Popen(["python", "kaggle/kaggle5.py"], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    outs, errs = ipynb.communicate()

    assert ipynb.returncode == 0, errs


def test_kaggle6(generate_dataset):
    columns = ["pixel{}".format(i) for i in range(784)]
    dtypes = [int for _ in range(784)]
    generate_dataset("kaggle/test.csv", columns, dtypes)

    ipynb = subprocess.Popen(["python", "kaggle/kaggle6.py"], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    outs, errs = ipynb.communicate()

    assert ipynb.returncode == 0, errs


def test_kaggle7(generate_dataset):
    columns = ['SK_ID_CURR', 'TARGET', 'NAME_CONTRACT_TYPE', 'CODE_GENDER',
       'FLAG_OWN_CAR', 'FLAG_OWN_REALTY', 'CNT_CHILDREN',
       'AMT_INCOME_TOTAL', 'AMT_CREDIT', 'AMT_ANNUITY', 'AMT_GOODS_PRICE',
       'NAME_TYPE_SUITE', 'NAME_INCOME_TYPE', 'NAME_EDUCATION_TYPE',
       'NAME_FAMILY_STATUS', 'NAME_HOUSING_TYPE',
       'REGION_POPULATION_RELATIVE', 'DAYS_BIRTH', 'DAYS_EMPLOYED',
       'DAYS_REGISTRATION', 'DAYS_ID_PUBLISH', 'OWN_CAR_AGE',
       'FLAG_MOBIL', 'FLAG_EMP_PHONE', 'FLAG_WORK_PHONE',
       'FLAG_CONT_MOBILE', 'FLAG_PHONE', 'FLAG_EMAIL', 'OCCUPATION_TYPE',
       'CNT_FAM_MEMBERS', 'REGION_RATING_CLIENT',
       'REGION_RATING_CLIENT_W_CITY', 'WEEKDAY_APPR_PROCESS_START',
       'HOUR_APPR_PROCESS_START', 'REG_REGION_NOT_LIVE_REGION',
       'REG_REGION_NOT_WORK_REGION', 'LIVE_REGION_NOT_WORK_REGION',
       'REG_CITY_NOT_LIVE_CITY', 'REG_CITY_NOT_WORK_CITY',
       'LIVE_CITY_NOT_WORK_CITY', 'ORGANIZATION_TYPE', 'EXT_SOURCE_1',
       'EXT_SOURCE_2', 'EXT_SOURCE_3', 'APARTMENTS_AVG',
       'BASEMENTAREA_AVG', 'YEARS_BEGINEXPLUATATION_AVG',
       'YEARS_BUILD_AVG', 'COMMONAREA_AVG', 'ELEVATORS_AVG',
       'ENTRANCES_AVG', 'FLOORSMAX_AVG', 'FLOORSMIN_AVG', 'LANDAREA_AVG',
       'LIVINGAPARTMENTS_AVG', 'LIVINGAREA_AVG',
       'NONLIVINGAPARTMENTS_AVG', 'NONLIVINGAREA_AVG', 'APARTMENTS_MODE',
       'BASEMENTAREA_MODE', 'YEARS_BEGINEXPLUATATION_MODE',
       'YEARS_BUILD_MODE', 'COMMONAREA_MODE', 'ELEVATORS_MODE',
       'ENTRANCES_MODE', 'FLOORSMAX_MODE', 'FLOORSMIN_MODE',
       'LANDAREA_MODE', 'LIVINGAPARTMENTS_MODE', 'LIVINGAREA_MODE',
       'NONLIVINGAPARTMENTS_MODE', 'NONLIVINGAREA_MODE',
       'APARTMENTS_MEDI', 'BASEMENTAREA_MEDI',
       'YEARS_BEGINEXPLUATATION_MEDI', 'YEARS_BUILD_MEDI',
       'COMMONAREA_MEDI', 'ELEVATORS_MEDI', 'ENTRANCES_MEDI',
       'FLOORSMAX_MEDI', 'FLOORSMIN_MEDI', 'LANDAREA_MEDI',
       'LIVINGAPARTMENTS_MEDI', 'LIVINGAREA_MEDI',
       'NONLIVINGAPARTMENTS_MEDI', 'NONLIVINGAREA_MEDI',
       'FONDKAPREMONT_MODE', 'HOUSETYPE_MODE', 'TOTALAREA_MODE',
       'WALLSMATERIAL_MODE', 'EMERGENCYSTATE_MODE',
       'OBS_30_CNT_SOCIAL_CIRCLE', 'DEF_30_CNT_SOCIAL_CIRCLE',
       'OBS_60_CNT_SOCIAL_CIRCLE', 'DEF_60_CNT_SOCIAL_CIRCLE',
       'DAYS_LAST_PHONE_CHANGE', 'FLAG_DOCUMENT_2', 'FLAG_DOCUMENT_3',
       'FLAG_DOCUMENT_4', 'FLAG_DOCUMENT_5', 'FLAG_DOCUMENT_6',
       'FLAG_DOCUMENT_7', 'FLAG_DOCUMENT_8', 'FLAG_DOCUMENT_9',
       'FLAG_DOCUMENT_10', 'FLAG_DOCUMENT_11', 'FLAG_DOCUMENT_12',
       'FLAG_DOCUMENT_13', 'FLAG_DOCUMENT_14', 'FLAG_DOCUMENT_15',
       'FLAG_DOCUMENT_16', 'FLAG_DOCUMENT_17', 'FLAG_DOCUMENT_18',
       'FLAG_DOCUMENT_19', 'FLAG_DOCUMENT_20', 'FLAG_DOCUMENT_21',
       'AMT_REQ_CREDIT_BUREAU_HOUR', 'AMT_REQ_CREDIT_BUREAU_DAY',
       'AMT_REQ_CREDIT_BUREAU_WEEK', 'AMT_REQ_CREDIT_BUREAU_MON',
       'AMT_REQ_CREDIT_BUREAU_QRT', 'AMT_REQ_CREDIT_BUREAU_YEAR']
    dtypes = [int, int, str, str, str, str, int, float, float, float, float, str,
    str, str, str, str, float, int, int, float, int, float, int, int, int, int,
    int, int, str, float, int, int, str, int, int, int, int, int, int, int, str,
    float, float, float, float, float, float, float, float, float, float, float,
    float, float, float, float, float, float, float, float, float, float, float,
    float, float, float, float, float, float, float, float, float, float, float,
    float, float, float, float, float, float, float, float, float, float, float,
    float, str, str, float, str, str, float, float, float, float, float, int, int,
    int, int, int, int, int, int, int, int, int, int, int, int, int, int, int, int,
    int, int, float, float, float, float, float, float]
    generate_dataset("kaggle/application_test.csv", columns, dtypes)
    generate_dataset("kaggle/application_train.csv", columns, dtypes)

    ipynb = subprocess.Popen(["python", "kaggle/kaggle7.py"], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    outs, errs = ipynb.communicate()

    assert ipynb.returncode == 0, errs


def test_kaggle8(generate_dataset):
    columns = ['Id', 'MSSubClass', 'MSZoning', 'LotFrontage', 'LotArea', 'Street',
       'Alley', 'LotShape', 'LandContour', 'Utilities', 'LotConfig',
       'LandSlope', 'Neighborhood', 'Condition1', 'Condition2',
       'BldgType', 'HouseStyle', 'OverallQual', 'OverallCond',
       'YearBuilt', 'YearRemodAdd', 'RoofStyle', 'RoofMatl',
       'Exterior1st', 'Exterior2nd', 'MasVnrType', 'MasVnrArea',
       'ExterQual', 'ExterCond', 'Foundation', 'BsmtQual', 'BsmtCond',
       'BsmtExposure', 'BsmtFinType1', 'BsmtFinSF1', 'BsmtFinType2',
       'BsmtFinSF2', 'BsmtUnfSF', 'TotalBsmtSF', 'Heating', 'HeatingQC',
       'CentralAir', 'Electrical', '1stFlrSF', '2ndFlrSF', 'LowQualFinSF',
       'GrLivArea', 'BsmtFullBath', 'BsmtHalfBath', 'FullBath',
       'HalfBath', 'BedroomAbvGr', 'KitchenAbvGr', 'KitchenQual',
       'TotRmsAbvGrd', 'Functional', 'Fireplaces', 'FireplaceQu',
       'GarageType', 'GarageYrBlt', 'GarageFinish', 'GarageCars',
       'GarageArea', 'GarageQual', 'GarageCond', 'PavedDrive',
       'WoodDeckSF', 'OpenPorchSF', 'EnclosedPorch', '3SsnPorch',
       'ScreenPorch', 'PoolArea', 'PoolQC', 'Fence', 'MiscFeature',
       'MiscVal', 'MoSold', 'YrSold', 'SaleType', 'SaleCondition']
    dtypes = [int, int, str, float, int, str, float, str, str, str, str, str,
    str, str, str, str, str, int, int, int, int, str, str, str, str, str, float,
    str, str, str, str, str, str, str, float, str, float, float, float, str, str,
    str, str, int, int, int, int, float, float, int, int, int, int, str, int, str,
    int, float, str, float, str, float, float, str, str, str, int, int, int, int,
    int, int, float, str, float, int, int, int, str, str]
    generate_dataset("kaggle/test.csv", columns, dtypes)
    generate_dataset("kaggle/train.csv", columns, dtypes)

    ipynb = subprocess.Popen(["python", "kaggle/kaggle8.py"], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    outs, errs = ipynb.communicate()

    assert ipynb.returncode == 0, errs


def test_kaggle9(generate_dataset):
    columns = ['Id', 'MSSubClass', 'MSZoning', 'LotFrontage', 'LotArea', 'Street',
       'Alley', 'LotShape', 'LandContour', 'Utilities', 'LotConfig',
       'LandSlope', 'Neighborhood', 'Condition1', 'Condition2', 'BldgType',
       'HouseStyle', 'OverallQual', 'OverallCond', 'YearBuilt', 'YearRemodAdd',
       'RoofStyle', 'RoofMatl', 'Exterior1st', 'Exterior2nd', 'MasVnrType',
       'MasVnrArea', 'ExterQual', 'ExterCond', 'Foundation', 'BsmtQual',
       'BsmtCond', 'BsmtExposure', 'BsmtFinType1', 'BsmtFinSF1',
       'BsmtFinType2', 'BsmtFinSF2', 'BsmtUnfSF', 'TotalBsmtSF', 'Heating',
       'HeatingQC', 'CentralAir', 'Electrical', '1stFlrSF', '2ndFlrSF',
       'LowQualFinSF', 'GrLivArea', 'BsmtFullBath', 'BsmtHalfBath', 'FullBath',
       'HalfBath', 'BedroomAbvGr', 'KitchenAbvGr', 'KitchenQual',
       'TotRmsAbvGrd', 'Functional', 'Fireplaces', 'FireplaceQu', 'GarageType',
       'GarageYrBlt', 'GarageFinish', 'GarageCars', 'GarageArea', 'GarageQual',
       'GarageCond', 'PavedDrive', 'WoodDeckSF', 'OpenPorchSF',
       'EnclosedPorch', '3SsnPorch', 'ScreenPorch', 'PoolArea', 'PoolQC',
       'Fence', 'MiscFeature', 'MiscVal', 'MoSold', 'YrSold', 'SaleType',
       'SaleCondition', 'SalePrice']
    dtypes = [int, int, str, float, int, str, float, str, str, str, str, str, str,
        str, str, str, str, int, int, int, int, str, str, str, str, str, float, str,
         str, str, str, str, str, str, int, str, int, int, int, str, str, str, str,
         int, int, int, int, int, int, int, int, int, int, str, int, str, int, float,
         str, float, str, int, int, str, str, str, int, int, int, int, int, int,
         float, float, float, int, int, int, str, str, int]
    generate_dataset("kaggle/test.csv", columns, dtypes)
    generate_dataset("kaggle/train.csv", columns, dtypes)

    ipynb = subprocess.Popen(["python", "kaggle/kaggle9.py"], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    outs, errs = ipynb.communicate()

    assert ipynb.returncode == 0, errs


def test_kaggle10(generate_dataset):
    columns = ['pelvic_incidence', 'pelvic_tilt numeric', 'lumbar_lordosis_angle',
       'sacral_slope', 'pelvic_radius', 'degree_spondylolisthesis', 'class']
    dtypes = [float, float, float, float, float, float, str]
    generate_dataset("kaggle/test.csv", columns, dtypes)
    generate_dataset("kaggle/train.csv", columns, dtypes)

    ipynb = subprocess.Popen(["python", "kaggle/kaggle10.py"], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    outs, errs = ipynb.communicate()

    assert ipynb.returncode == 0, errs


def test_kaggle12(generate_dataset):
    columns = ['PassengerId', 'Pclass', 'Name', 'Sex', 'Age', 'SibSp', 'Parch',
    'Ticket', 'Fare', 'Cabin', 'Embarked']
    dtypes = [int, int, str, str, float, int, int, str, float, float, str]
    generate_dataset("kaggle/train.csv", columns, dtypes)
    generate_dataset("kaggle/test.csv", columns, dtypes)

    ipynb = subprocess.Popen(["python", "kaggle/kaggle12.py"], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    outs, errs = ipynb.communicate()

    assert ipynb.returncode == 0, errs


def test_kaggle13(generate_dataset):
    columns = ['Id', 'SepalLengthCm', 'SepalWidthCm', 'PetalLengthCm',
    'PetalWidthCm', 'Species']
    dtypes = [int, float, float, float, float, str]
    generate_dataset("kaggle/Iris.csv", columns, dtypes)

    ipynb = subprocess.Popen(["python", "kaggle/kaggle13.py"], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    outs, errs = ipynb.communicate()

    assert ipynb.returncode == 0, errs


def test_kaggle14(generate_dataset):
    columns = ['PassengerId', 'Pclass', 'Name', 'Sex', 'Age', 'SibSp', 'Parch',
    'Ticket', 'Fare', 'Cabin', 'Embarked']
    dtypes = [int, int, str, str, float, int, int, str, float, float, str]
    generate_dataset("kaggle/test.csv", columns, dtypes)
    generate_dataset("kaggle/train.csv", columns, dtypes)

    ipynb = subprocess.Popen(["python", "kaggle/kaggle14.py"], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    outs, errs = ipynb.communicate()

    assert ipynb.returncode == 0, errs


def test_kaggle17(generate_dataset):
    columns = ['Suburb', 'Address', 'Rooms', 'Type', 'Price', 'Method', 'SellerG',
       'Date', 'Distance', 'Postcode', 'Bedroom2', 'Bathroom', 'Car', 'Landsize',
       'BuildingArea', 'YearBuilt', 'CouncilArea', 'Lattitude', 'Longtitude',
       'Regionname', 'Propertycount']
    dtypes = [str, str, int, str, float, str, str, str, float, float, float,
    float, float, float, float, float, str, float, float, str, float]
    generate_dataset("kaggle/test.csv", columns, dtypes)
    generate_dataset("kaggle/train.csv", columns, dtypes)

    ipynb = subprocess.Popen(["python", "kaggle/kaggle17.py"], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    outs, errs = ipynb.communicate()

    assert ipynb.returncode == 0, errs


def test_kaggle18(generate_dataset):
    columns = ['train_id', 'name', 'item_condition_id', 'category_name',
       'brand_name', 'price', 'shipping', 'item_description']
    dtypes = [int, str, int, str, float, float, int, str]
    generate_dataset("kaggle/test.csv", columns, dtypes)
    generate_dataset("kaggle/train.csv", columns, dtypes)

    ipynb = subprocess.Popen(["python", "kaggle/kaggle18.py"], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    outs, errs = ipynb.communicate()

    assert ipynb.returncode == 0, errs


def test_kaggle19(generate_dataset):
    columns = ['Id', 'groupId', 'matchId', 'assists', 'boosts', 'damageDealt',
       'DBNOs', 'headshotKills', 'heals', 'killPlace', 'killPoints',
       'kills', 'killStreaks', 'longestKill', 'matchDuration',
       'matchType', 'maxPlace', 'numGroups', 'rankPoints', 'revives',
       'rideDistance', 'roadKills', 'swimDistance', 'teamKills',
       'vehicleDestroys', 'walkDistance', 'weaponsAcquired', 'winPoints']
    dtypes = [str, str, str, int, int, float, int, int, int, int, int, int, int,
    float, int, str, int, int, int, int, float, int, float, int, int, float,
    int, int]
    generate_dataset("kaggle/train.csv", columns, dtypes)

    ipynb = subprocess.Popen(["python", "kaggle/kaggle19.py"], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    outs, errs = ipynb.communicate()

    assert ipynb.returncode == 0, errs


def test_kaggle20(generate_dataset):
    columns = ['id', 'diagnosis', 'radius_mean', 'texture_mean', 'perimeter_mean',
       'area_mean', 'smoothness_mean', 'compactness_mean',
       'concavity_mean', 'concave points_mean', 'symmetry_mean',
       'fractal_dimension_mean', 'radius_se', 'texture_se',
       'perimeter_se', 'area_se', 'smoothness_se', 'compactness_se',
       'concavity_se', 'concave points_se', 'symmetry_se',
       'fractal_dimension_se', 'radius_worst', 'texture_worst',
       'perimeter_worst', 'area_worst', 'smoothness_worst',
       'compactness_worst', 'concavity_worst', 'concave points_worst',
       'symmetry_worst', 'fractal_dimension_worst', 'Unnamed: 32']
    dtypes = [int, str, float, float, float, float, float, float, float, float,
    float, float, float, float, float, float, float, float, float, float, float,
    float, float, float, float, float, float, float, float, float, float, float,
    float]
    generate_dataset("kaggle/test.csv", columns, dtypes)
    generate_dataset("kaggle/train.csv", columns, dtypes)

    ipynb = subprocess.Popen(["python", "kaggle/kaggle20.py"], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    outs, errs = ipynb.communicate()

    assert ipynb.returncode == 0, errs


def test_kaggle21(generate_dataset):
    columns =
    dtypes =
    generate_dataset("kaggle/test.csv", columns, dtypes)
    generate_dataset("kaggle/train.csv", columns, dtypes)

    ipynb = subprocess.Popen(["python", "kaggle/kaggle21.py"], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    outs, errs = ipynb.communicate()

    assert ipynb.returncode == 0, errs


def test_kaggle22(generate_dataset):
    columns =
    dtypes =
    generate_dataset("kaggle/test.csv", columns, dtypes)
    generate_dataset("kaggle/train.csv", columns, dtypes)

    ipynb = subprocess.Popen(["python", "kaggle/kaggle22.py"], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    outs, errs = ipynb.communicate()

    assert ipynb.returncode == 0, errs


def test_kaggle23(generate_dataset):
    columns =
    dtypes =
    generate_dataset("kaggle/test.csv", columns, dtypes)
    generate_dataset("kaggle/train.csv", columns, dtypes)

    ipynb = subprocess.Popen(["python", "kaggle/kaggle23.py"], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    outs, errs = ipynb.communicate()

    assert ipynb.returncode == 0, errs
