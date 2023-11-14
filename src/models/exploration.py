from datetime import timedelta

from data.old_code.clean import clean_fire_data
import statsmodels.tsa.api as smt
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns

from sklearn.preprocessing import MinMaxScaler


def time_plot(data, x_col, y_col, title):
    fig, ax = plt.subplots(figsize=(15, 5))
    sns.lineplot(
        x=x_col, y=y_col, data=data, ax=ax, color="mediumblue", label="Total Incidents"
    )
    # sns.relplot(data=data, x=x_col, y=y_col)

    second = data.groupby(data.date.dt.year)[y_col].mean().reset_index()
    second.date = pd.to_datetime(second.date, format="%Y")
    x_mean_col = second.date + timedelta(6 * 365 / 12)
    sns.lineplot(
        x=x_mean_col, y=y_col, data=second, ax=ax, color="red", label="Mean Incidents"
    )

    ax.set(xlabel="Date", ylabel="Incident Count", title=title)

    sns.despine()


def get_diff(data):
    data["incident_diff"] = data.incident_count.diff()
    data = data.dropna()

    # data.to_csv('../data/stationary_df.csv')
    return data


def plots(data, lags=None):
    # Convert dataframe to datetime index
    dt_data = data.set_index("date").drop("incident_count", axis=1)
    dt_data.dropna(axis=0)

    layout = (1, 3)
    fig, ax = plt.subplots()
    raw = plt.subplot2grid(layout, (0, 0))
    acf = plt.subplot2grid(layout, (0, 1))
    pacf = plt.subplot2grid(layout, (0, 2))

    second = data.groupby(data.date.dt.year)["incident_diff"].mean().reset_index()
    second.date = pd.to_datetime(second.date, format="%Y").dt.date
    second.date = second.date + timedelta(6 * 365 / 12)

    sns.lineplot(
        x="date",
        y="incident_diff",
        data=second,
        ax=raw,
        color="red",
        label="Mean Incidents",
    )
    dt_data.plot(ax=raw, figsize=(12, 5), color="mediumblue")
    smt.graphics.plot_acf(dt_data, lags=lags, ax=acf, color="mediumblue")
    smt.graphics.plot_pacf(dt_data, lags=lags, ax=pacf, color="mediumblue")
    sns.despine()
    plt.tight_layout()


# create dataframe for transformation from time series to supervised
def generate_supervised(data: pd.DataFrame, nb_of_lags: int):
    supervised_df = data.copy()

    # create column for each lag
    for i in range(1, nb_of_lags):
        col_name = "lag_" + str(i)
        supervised_df[col_name] = supervised_df["incident_count"].shift(i)

    # drop null values
    supervised_df = supervised_df.dropna().reset_index(drop=True)

    # supervised_df.to_csv('../data/model_df.csv', index=False)

    return supervised_df


def generate_arima_data(data):
    dt_data = data.set_index("date").drop("incident_count", axis=1)
    dt_data.dropna(axis=0)

    # dt_data.to_csv('../data/arima_df.csv')

    return dt_data


def tts(data):
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

    X_train, y_train = train_set_scaled[:, 1:], train_set_scaled[:, 0:1].ravel()
    X_test, y_test = test_set_scaled[:, 1:], test_set_scaled[:, 0:1].ravel()

    return X_train, y_train, X_test, y_test, scaler


def data_exploration():
    # get clearn dataset
    fire_inc_mtl = clean_fire_data()

    # drop actual NOT fires
    fire_inc_mtl = fire_inc_mtl[fire_inc_mtl["TYPE"] != "C"]

    # Drop the day indicator from the date column
    fire_inc_mtl = fire_inc_mtl.rename(columns={"DATE": "date"})
    fire_inc_mtl.date = fire_inc_mtl.date.apply(lambda x: str(x)[:-3])

    # aggregate by month
    monthly_df = fire_inc_mtl.groupby(["date"])["INCIDENT_NBR"].count().reset_index()
    monthly_df.date = pd.to_datetime(monthly_df.date)
    monthly_df = monthly_df.rename(columns={"INCIDENT_NBR": "incident_count"})

    # display
    time_plot(
        monthly_df,
        "date",
        "incident_count",
        "Monthly Incidents Before Diff Transformation",
    )

    # get stationary
    stationary_df = get_diff(monthly_df)
    time_plot(
        stationary_df,
        "date",
        "incident_diff",
        "Monthly Incidents After Diff Transformation",
    )

    # print correlation analysis
    plots(stationary_df, lags=24)

    # get data
    model_df = generate_supervised(stationary_df, 11)
    datetime_df = generate_arima_data(stationary_df)

    train, test = tts(model_df)
    X_train, y_train, X_test, y_test, scaler_object = scale_data(train, test)

    print("Completed!!")
