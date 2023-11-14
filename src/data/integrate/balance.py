from sklearn import metrics
from sklearn.model_selection import train_test_split
from collections import Counter
from sklearn.metrics import precision_score
from sklearn.metrics import recall_score
from sklearn.metrics import f1_score
from sklearn.metrics import accuracy_score
from sklearn.metrics import roc_curve, auc
from sklearn.metrics import classification_report
from scikitplot.metrics import plot_roc
from scikitplot.metrics import plot_precision_recall
from scikitplot.metrics import plot_cumulative_gain
from scikitplot.metrics import plot_lift_curve
from numpy import argmax
from imblearn.over_sampling import SMOTE
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import xgboost as xgb
import datetime

from data.old_code.visualize.visualizations import view_model_evaluation


def build_and_test(
    X_train: pd.DataFrame,
    X_test: pd.DataFrame,
    y_train,
    y_test,
    model_obj,
    model_name,
    threshold=False,
    is_show_metrics: bool = False,
):
    # print statistics
    print(f"MODEL: '{model_name}'\n")

    print(f"Training-set target label count: {Counter(y_train)}")
    print(f"Testing-set target label count : {Counter(y_test)}\n")

    model = model_obj

    # Build and fit the model
    model.fit(X_train, y_train)

    # Test the model
    y_pred = model.predict(X_test)
    print(
        "Precision score: {0:.2f}".format(
            precision_score(y_test, y_pred, average="weighted")
        )
    )
    print(
        "Recall score:    {0:.2f}".format(
            recall_score(y_test, y_pred, average="weighted")
        )
    )
    print(
        "F1-score score:  {0:.2f}".format(f1_score(y_test, y_pred, average="weighted"))
    )
    print("Accuracy score:  {0:.2f}\n".format(accuracy_score(y_test, y_pred)))

    data = X_test.copy()
    data["Y_TEST"] = y_test
    data["Y_PRED"] = y_pred

    y_score = model.predict_proba(X_test)
    # score_binary_classification(threshold, y_score, y_test)

    # Plot metrics
    plot_roc(y_test, y_score)
    plt.title(f"ROC Curves - {model_name}")
    plt.show()

    plot_precision_recall(y_test, y_score)
    plt.title(f"Precision-Recall Curve - {model_name}")
    plt.show()

    print(metrics.confusion_matrix(y_test, y_pred))

    # Print a classification report
    print(classification_report(y_test, y_pred))
    # return roc_auc0, #fpr0, tpr0, best_threshold
    return model


def score_binary_classification(threshold, y_score, y_test):
    fpr0, tpr0, thresholds = roc_curve(y_test, y_score[:, 1])
    roc_auc0 = auc(fpr0, tpr0)
    # Calculate the best threshold
    best_threshold = None
    if threshold:
        J = tpr0 - fpr0
        ix = argmax(J)  # take the value which maximizes the J variable
        best_threshold = thresholds[ix]
        # adjust score according to threshold.
        y_score = np.array(
            [[1, y[1]] if y[0] >= best_threshold else [0, y[1]] for y in y_score]
        )
    # Plot metrics
    plot_roc(y_test, y_score)
    plt.show()
    plot_precision_recall(y_test, y_score)
    plt.show()
    plot_cumulative_gain(y_test, y_score)
    plt.show()
    plot_lift_curve(y_test, y_score)
    plt.show()


def validate_models(
    x_train: pd.DataFrame,
    x_test: pd.DataFrame,
    x_res: pd.DataFrame,
    y_train: pd.DataFrame,
    y_test: pd.DataFrame,
    y_res: pd.DataFrame,
    model,
    model_name,
):
    roc_auc_imb, fpr_imb, tpr_imb, _ = build_and_test(
        x_train, x_test, y_train.ravel(), y_test.ravel(), model, model_name
    )
    print("-----------------------------------------------------------------")
    roc_auc_imb, fpr_imb, tpr_imb, _ = build_and_test(
        x_res, x_test, y_res.ravel(), y_test.ravel(), model, model_name + " - SMOT"
    )


def evaluate_models(x: pd.DataFrame, y: pd.DataFrame, eval_dataset: pd.DataFrame):
    #
    x_train, x_test, y_train, y_test = train_test_split(
        x, y, test_size=0.3, random_state=100
    )

    over_sampler = SMOTE(k_neighbors=2)
    x_res, y_res = over_sampler.fit_resample(x_train, y_train)

    # print("=================================================================")
    # build_and_test(
    #     x_train,
    #     x_test,
    #     y_train.ravel(),
    #     y_test.ravel(),
    #     LogisticRegression(),
    #     'Logistic Regression'
    # )
    # print("-----------------------------------------------------------------")
    # build_and_test(
    #     x_res,
    #     x_test,
    #     y_res.ravel(),
    #     y_test.ravel(),
    #     LogisticRegression(),
    #     'Logistic Regression w/ SMOTE'
    # )
    # print("=================================================================")
    # build_and_test(
    #     x_train,
    #     x_test,
    #     y_train.ravel(),
    #     y_test.ravel(),
    #     DecisionTreeClassifier(),
    #     'Decision Tree Classifier'
    # )
    # print("-----------------------------------------------------------------")
    # build_and_test(
    #     x_res,
    #     x_test,
    #     y_res.ravel(),
    #     y_test.ravel(),
    #     DecisionTreeClassifier(),
    #     'Decision Tree Classifier w/ SMOTE'
    # )
    # print("=================================================================")
    # build_and_test(
    #     x_train,
    #     x_test,
    #     y_train.ravel(),
    #     y_test.ravel(),
    #     RandomForestClassifier(),
    #     'Random Forest Classifier',
    # )
    # print("-----------------------------------------------------------------")
    # build_and_test(
    #     x_res,
    #     x_test,
    #     y_res.ravel(),
    #     y_test.ravel(),
    #     RandomForestClassifier(),
    #     'Random Forest Classifier w/ SMOTE'
    # )
    # print("=================================================================")
    # build_and_test(
    #     x_train,
    #     x_test,
    #     y_train.ravel(),
    #     y_test.ravel(),
    #     xgb.XGBClassifier(),
    #     'XGBoost',
    # )
    print("-----------------------------------------------------------------")
    model = build_and_test(
        x_res,
        x_test,
        y_res.ravel(),
        y_test.ravel(),
        xgb.XGBClassifier(),
        "XGBoost w/ SMOTE",
    )
    print("=================================================================")
    validate(model, "XGBoost w/ SMOTE", eval_dataset)


def validate(model, model_name: str, eval_dataset: pd.DataFrame):
    feat_importance = model.get_booster().get_score(importance_type="weight")

    feat_keys = list(feat_importance.keys())
    feat_vals = list(feat_importance.values())

    feat_data = pd.DataFrame(
        data=feat_vals, index=feat_keys, columns=["score"]
    ).sort_values(by="score", ascending=True)
    feat_data = feat_data.reset_index(drop=False).rename(columns={"index": "feature"})
    plt.barh("feature", "score", data=feat_data)
    plt.grid(axis="x")

    dates = eval_dataset["DATE"]

    x_eval = eval_dataset.drop(
        ["YEAR", "DATE", "QUARTER", "FIRES_YES_COUNT", "IS_FIRE", "RISK"], axis=1
    )
    y_eval = eval_dataset["RISK"]

    data = x_eval.copy()
    y_pred = model.predict(x_eval)
    data["Y_VAL"] = y_eval
    data["Y_PRED"] = y_pred
    data["DATE"] = dates

    view_model_evaluation(data)

    print("Done!")


def get_risk_label(value_count: int) -> int:
    # default value
    ret_val = 99

    # classify value
    if value_count == 0:
        # low risk
        ret_val = 0
    elif value_count == 1:
        # low risk
        ret_val = 0
    elif value_count == 2:
        # mid risk
        ret_val = 0
    elif value_count == 3:
        # mid risk
        ret_val = 1
    elif value_count == 4:
        # mid risk
        ret_val = 1
    elif value_count >= 5:
        # high
        ret_val = 2

    return ret_val


def balance_data():
    final_dataset_path = "out/model_data/final_dataset/03-dataset-clean_fill-median_quarterly_grid_500m.csv"

    final_dataset = pd.read_csv(final_dataset_path, parse_dates=["DATE"])

    final_dataset = final_dataset.astype(
        {
            "FIRES_YES_COUNT": "int",
            "FIRES_NO_COUNT": "int",
            "CRIME_DEAD_COUNT": "int",
            "CRIME_INTR_COUNT": "int",
            "CRIME_MISD_COUNT": "int",
            "CRIME_CAR_W_THEFT_COUNT": "int",
            "CRIME_CAR_THEFT_COUNT": "int",
            "CRIME_ROBBERY_COUNT": "int",
            "NB_TAX_PARCELS": "int",
            "N_LOGEMENT_SUM": "int",
            "COMMERCIAL": "int",
            "RESIDENTIAL": "int",
            "N_BUILDINGS": "int",
        }
    )

    final_dataset_raw = final_dataset.copy()

    fire_col_names = [
        x for x in final_dataset_raw.columns.values if x.startswith("FIRES_")
    ]
    crime_col_names = [
        x for x in final_dataset_raw.columns.values if x.startswith("CRIME_")
    ]

    final_dataset = final_dataset.sort_values(
        by=["grid_id", "YEAR", "QUARTER"], ascending=True
    )

    for col in fire_col_names:
        final_dataset[col] = final_dataset.groupby("grid_id")[col].shift()

    for col in crime_col_names:
        final_dataset[col] = final_dataset.groupby("grid_id")[col].shift()

    final_dataset["IS_FIRE"] = (
        final_dataset["FIRES_YES_COUNT"].astype("bool").astype("int8")
    )

    final_dataset = final_dataset.dropna(how="any").reset_index(drop=True)

    final_dataset = final_dataset.astype(
        {
            "FIRES_YES_COUNT": "int64",
            "FIRES_NO_COUNT": "int64",
            "CRIME_DEAD_COUNT": "int64",
            "CRIME_INTR_COUNT": "int64",
            "CRIME_MISD_COUNT": "int64",
            "CRIME_CAR_W_THEFT_COUNT": "int64",
            "CRIME_CAR_THEFT_COUNT": "int64",
            "CRIME_ROBBERY_COUNT": "int64",
        }
    )

    final_dataset_risk = [get_risk_label(x) for x in final_dataset["FIRES_YES_COUNT"]]
    df_num = final_dataset.drop(["grid_id", "YEAR", "DATE", "QUARTER"], axis=1)

    # scaler = MinMaxScaler()
    # df_norm = pd.DataFrame(scaler.fit_transform(df_num), columns=df_num.columns)
    #
    # final_dataset[df_norm.columns] = df_norm
    final_dataset["RISK"] = final_dataset_risk

    final_dataset = final_dataset.sort_values(
        by=["grid_id", "YEAR", "QUARTER"], ascending=True
    )
    previous_year = datetime.datetime.today() - datetime.timedelta(days=365)

    train_test_dataset = final_dataset[final_dataset["DATE"] < previous_year]
    eval_dataset = final_dataset[final_dataset["DATE"] >= previous_year]

    x = train_test_dataset.drop(
        ["YEAR", "DATE", "QUARTER", "FIRES_YES_COUNT", "IS_FIRE", "RISK"], axis=1
    )
    y = train_test_dataset["RISK"]

    evaluate_models(x=x, y=y, eval_dataset=eval_dataset)

    print("Hello!")
