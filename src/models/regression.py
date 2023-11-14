import pandas as pd
import numpy as np

import matplotlib.pyplot as plt
import seaborn as sns

from sklearn.preprocessing import MinMaxScaler
from sklearn.linear_model import LinearRegression
from sklearn.metrics import mean_squared_error, mean_absolute_error, r2_score
from sklearn.ensemble import RandomForestRegressor
from xgboost.sklearn import XGBRegressor
import statsmodels.formula.api as smf

#
# import keras
# from keras.layers import Dense
# from keras.models import Sequential
# from keras.optimizers import Adam
# from keras.callbacks import EarlyStopping
# from keras.utils import np_utils
# from keras.layers import LSTM

from data.old_code.fetch import fire_incidents_data
from models.exploration import get_diff, generate_supervised, generate_arima_data

model_scores = {}


def tts(data: pd.DataFrame):
    data = data.drop(["incident_count", "date"], axis=1)
    train, test = data[0:-12].values, data[-12:].values

    return train, test


def scale_data(train_set, test_set):
    # apply Min Max Scaler
    scaler = MinMaxScaler(feature_range=(-1, 1))
    scaler = scaler.fit(train_set)

    # reshape training set
    train_set = train_set.reshape(train_set.shape[0], train_set.shape[1])
    train_set_scaled = scaler.transform(train_set)

    # reshape test set
    test_set = test_set.reshape(test_set.shape[0], test_set.shape[1])
    test_set_scaled = scaler.transform(test_set)

    x_train, y_train = train_set_scaled[:, 1:], train_set_scaled[:, 0:1].ravel()
    x_test, y_test = test_set_scaled[:, 1:], test_set_scaled[:, 0:1].ravel()

    return x_train, y_train, x_test, y_test, scaler


def undo_scaling(y_pred, x_test, scaler_obj, lstm=False):
    # reshape y_pred
    y_pred = y_pred.reshape(y_pred.shape[0], 1, 1)

    if not lstm:
        x_test = x_test.reshape(x_test.shape[0], 1, x_test.shape[1])

    # rebuild test set for inverse transform
    pred_test_set = []
    for index in range(0, len(y_pred)):
        pred_test_set.append(np.concatenate([y_pred[index], x_test[index]], axis=1))

    # reshape pred_test_set
    pred_test_set = np.array(pred_test_set)
    pred_test_set = pred_test_set.reshape(
        pred_test_set.shape[0], pred_test_set.shape[2]
    )

    # inverse transform
    pred_test_set_inverted = scaler_obj.inverse_transform(pred_test_set)

    return pred_test_set_inverted


def load_original_df():
    # get raw
    fire_inc_mtl = fire_incidents_data(remove_unrelevant=True, add_time_categories=True)

    # drop actual NOT fires
    fire_inc_mtl = fire_inc_mtl[fire_inc_mtl["TYPE"] != "C"]

    # Drop the day indicator from the date column
    fire_inc_mtl = fire_inc_mtl.rename(columns={"DATE": "date"})
    fire_inc_mtl.date = fire_inc_mtl.date.apply(lambda x: str(x)[:-3])

    # aggregate by month
    monthly_df = fire_inc_mtl.groupby(["date"])["INCIDENT_NBR"].count().reset_index()
    monthly_df.date = pd.to_datetime(monthly_df.date)
    monthly_df = monthly_df.rename(columns={"INCIDENT_NBR": "incident_count"})

    return monthly_df


def predict_df(unscaled_predictions, original_df):
    # create dataframe that shows the predicted sales
    result_list = []
    sales_dates = list(original_df[-13:].date)
    act_incidents = list(original_df[-13:].incident_count)

    for index in range(0, len(unscaled_predictions)):
        result_dict = {}
        result_dict["pred_incidents"] = int(
            unscaled_predictions[index][0] + act_incidents[index]
        )
        result_dict["date"] = sales_dates[index + 1]
        result_list.append(result_dict)

    df_result = pd.DataFrame(result_list)

    return df_result


def get_scores(unscaled_df, original_df, model_name):
    rmse = np.sqrt(
        mean_squared_error(
            original_df.incident_count[-12:], unscaled_df.pred_incidents[-12:]
        )
    )
    mae = mean_absolute_error(
        original_df.incident_count[-12:], unscaled_df.pred_incidents[-12:]
    )
    r2 = r2_score(original_df.incident_count[-12:], unscaled_df.pred_incidents[-12:])
    model_scores[model_name] = [rmse, mae, r2]

    print(f"Model Name: {model_name}")
    print(f"RMSE: {rmse}")
    print(f"MAE: {mae}")
    print(f"R2 Score: {r2}")
    print("-----------------------------------------------")


def plot_model_predictions(results, original_df, model_name):
    fig, ax = plt.subplots(figsize=(15, 5))
    sns.lineplot(
        x=original_df.date,
        y=original_df.incident_count,
        data=original_df,
        ax=ax,
        label="Original",
        color="mediumblue",
    )
    sns.lineplot(
        x=results.date,
        y=results.pred_incidents,
        data=results,
        ax=ax,
        label="Predicted",
        color="Red",
    )

    ax.set(
        xlabel="Date",
        ylabel="Incidents",
        title=f"{model_name} Incidents Forecasting Prediction",
    )

    ax.legend()
    plt.grid()
    sns.despine()

    # plt.savefig(f'../model_output/{model_name}_forecast.png')


def plot_compared_results(results, original_df):
    fig, ax = plt.subplots(figsize=(15, 5))

    sns.lineplot(
        x=original_df.date,
        y=original_df.incident_count,
        data=original_df,
        ax=ax,
        label="Original",
        color="mediumblue",
        linewidth=3,
    )

    for key in results.keys():
        sns.lineplot(
            x=results[key].date,
            y=results[key].pred_incidents,
            data=results[key],
            ax=ax,
            label=key,
        )

    ax.set(
        xlabel="Date", ylabel="Incidents", title=f"Incidents Forecasting Predictions"
    )

    ax.legend()
    plt.grid()
    sns.despine()

    # plt.savefig(f'../model_output/{model_name}_forecast.png')


def run_model(train_data, test_data, model, model_name):
    x_train, y_train, x_test, y_test, scaler_object = scale_data(train_data, test_data)

    mod = model
    mod.fit(x_train, y_train)
    predictions = mod.predict(x_test)

    # Undo scaling to compare predictions against original data
    original_df = load_original_df()
    unscaled = undo_scaling(predictions, x_test, scaler_object)
    unscaled_df = predict_df(unscaled, original_df)

    get_scores(unscaled_df, original_df, model_name)

    plot_model_predictions(unscaled_df, original_df, model_name)

    return unscaled_df


def calculate_importance(
    model_df: pd.DataFrame,
    formula_prefix: str = "incident_diff",
    formula_suffix: str = "lag_1",
) -> float:
    # create formula
    formula = f"{formula_prefix} ~ {formula_suffix}"

    # Define the regression formula
    model = smf.ols(formula=formula, data=model_df)

    # Fit the regression
    model_fit = model.fit()

    # Extract the adjusted r-squared
    regression_adj_rsq = model_fit.rsquared_adj

    return regression_adj_rsq


def get_importance(model_df=pd.DataFrame):
    cols = model_df.columns.values

    diff_col_name = [x for x in cols if "diff" in x][0]
    lags_col_name = [x for x in cols if "lag_" in x]

    lag_importance = []
    suffix = ""

    for lag in lags_col_name:
        suffix = suffix + lag

        curr = calculate_importance(
            # noinspection PyTypeChecker
            model_df=model_df,
            formula_prefix=diff_col_name,
            formula_suffix=suffix,
        )
        print(lag + " : " + str(curr))
        lag_importance.append((lag, curr))

        suffix = suffix + " + "


def create_model():
    print(
        "       ___                            _   \n"
        "      / __\__  _ __ ___  ___ __ _ ___| |_ \n"
        "     / _\/ _ \| '__/ _ \/ __/ _` / __| __|\n"
        "    / / | (_) | | |  __/ (_| (_| \__ \ |_ \n"
        "    \/   \___/|_|  \___|\___\__,_|___/\__|"
    )
    print("===============================================")

    # get clearn dataset
    monthly_df = load_original_df()

    # get stationary
    stationary_df = get_diff(monthly_df)

    nb_lags = 12

    # get data
    model_df = generate_supervised(stationary_df, nb_lags + 1)
    datetime_df = generate_arima_data(stationary_df)

    train, test = tts(model_df)
    X_train, y_train, X_test, y_test, scaler_object = scale_data(train, test)

    results = {}

    pred = run_model(train, test, LinearRegression(), "LinearRegression")

    results["LinearRegression"] = pred

    pred = run_model(
        train,
        test,
        RandomForestRegressor(n_estimators=100, max_depth=20),
        "RandomForest",
    )

    results["RandomForest"] = pred

    pred = run_model(
        train,
        test,
        XGBRegressor(n_estimators=100, learning_rate=0.2, objective="reg:squarederror"),
        "XGBoost",
    )

    results["XGBoost"] = pred

    plot_compared_results(results, monthly_df.tail(nb_lags))
