# %%
import streamlit as st
import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt
from operator import attrgetter
from st_files_connection import FilesConnection
from datetime import datetime, timedelta
import os

# %%
# Récupération des secrets pour la base de données
db_username = st.secrets["db_username"]
db_password = st.secrets["db_password"]
# Récupération des secrets pour S3
s3_access_key = st.secrets["AWS_S3_credentials"]["access_key"]
s3_secret_key = st.secrets["AWS_S3_credentials"]["secret_key"]

# Create connection object and retrieve file contents.
# Specify input format is a csv and to cache the result for 600 seconds.
conn = st.experimental_connection("s3", type=FilesConnection)
orders = conn.read("one-data-lake/orders.csv", input_format="csv", ttl=600)

order_details = conn.read(
    "one-data-lake/order_details.csv", input_format="csv", ttl=600
)
users = conn.read("one-data-lake/", input_format="users.csv", ttl=600)
external_pmi = conn.read("one-data-lake/", input_format="pmi_external.csv", ttl=600)


# Print results.
for row in orders.itertuples():
    st.write(f"{row.Owner} has a :{row.Pet}:")

# %%
pd.set_option("display.max_columns", None)
pd.set_option("display.precision", 0)

# orders = pd.read_csv("orders.csv", delimiter=",", low_memory=False)
orders["order_id"] = orders["order_id"].astype(str)
orders["customer_id"] = orders["customer_id"].astype(str)
orders["createdAt"] = pd.to_datetime(orders["createdAt"])
orders = orders.rename(columns={"job_status": "Status"})
orders = orders[~orders["Status"].isin(["ABANDONED"])]
orders["customer_id"] = orders["customer_id"].str.rstrip(".0")
orders["businessCat"] = orders["businessCat"].replace(
    ["Recharge mobile", "Recharge mobile / ADSL"], ["Airtime", "Airtime"]
)
orders["customer_origine"] = orders["paymentType"].apply(
    lambda x: "Diaspora" if x == "CARD_PAY" else "Local"
)
orders = orders[
    ~orders["order_id"].isin(
        [
            "734138951872",
            "811738356736",
            "648042957760",
            "239046556928",
            "423486580736",
            "536463465088",
        ]
    )
]
orders = orders[
    ~orders["customer_id"].isin(
        [
            "2059318.0",
            "1506025442.0",
            "1694397201.0",
            "2830181885.0",
            "5620828389.0",
            "4064611739.0",
            "3385745613.0",
            "2281370.0",
            "64438759505.0",
            "569994573568.0",
            "1628682.0",
            "310179181696.0",
            "878446.0",
            "3643707.0",
            "2253354.0",
            "1771017743.0",
            "727840660224.0",
            "2280761953.0",
            "2864429.0",
            "1505970032.0",
            "1517116.0",
            "929482210496.0",
            "5884716233.0",
            "22781605568.0",
            "2794629.0",
            "47201675489.0",
            "6072524763.0",
            "2342577.0",
            "1440074.0",
            "3666483.0",
            "449701472960.0",
            "869120.0",
            "7304625963.0",
            "2214784702.0",
            "869883.0",
            "2851778338.0",
            "3000794.0",
            "1898245261.0",
            "9816298466.0",
            "7021529167.0",
            "3017838801.0",
            "5624710564.0",
            "1584024035.0",
            "2485567.0",
            "2763532338.0",
            "841024809600.0",
            "1739473.0",
            "2183725.0",
            "3788062.0",
            "23400912794.0",
            "150321448192.0",
            "461317394880.0",
            "2208215.0",
            "3669307840.0",
            "610335616576.0",
            "7478577450.0",
            "13153632574.0",
            "2815691755.0",
            "879984.0",
            "3312616.0",
            "548088380288.0",
            "3526036.0",
            "2367635120.0",
            "24957125457.0",
            "459557812544.0",
            "1290757210.0",
            "507345740736.0",
            "2558315057.0",
            "819751.0",
            "407181581440.0",
            "1412707541.0",
            "1419613392.0",
            "4068655.0",
            "303655560704.0",
            "2389210.0",
            "2765139.0",
            "504153462208.0",
            "2100305133.0",
            "653243920384.0",
            "1253878877.0",
            "43255929830.0",
        ]
    )
]
orders = orders.rename(columns={"Order Type": "Order_Type"})
orders.loc[(orders["customer_id"] == "73187559488.0"), "Order_Type"] = "EXTERNE"

orders_pmi = orders[orders["Order_Type"] == "EXTERNE"]

external_pmi = pd.read_csv("pmi_external.csv", delimiter=",", low_memory=False)
external_pmi["order_id"] = external_pmi["order_id"].astype(str)
external_pmi["customer_id"] = external_pmi["customer_id"].astype(str)
external_pmi = external_pmi.rename(columns={"job_status": "Status"})
external_pmi = external_pmi[~external_pmi["Status"].isin(["ABANDONED"])]
external_pmi["createdAt"] = pd.to_datetime(external_pmi["createdAt"])
external_pmi["customer_id"] = external_pmi["customer_id"].str.rstrip(".0")
external_pmi["businessCat"] = external_pmi["businessCat"].replace(
    ["Recharge mobile", "Recharge mobile / ADSL"], ["Airtime", "Airtime"]
)

# order_details = pd.read_csv("order_details.csv", delimiter=",", low_memory=False)
order_details["order_id"] = order_details["order_id"].astype(str)
order_details["customer_id"] = order_details["customer_id"].astype(str)
order_details["createdAt"] = pd.to_datetime(order_details["createdAt"])
order_details["product_id"] = order_details["product_id"].apply(int)
order_details = order_details.rename(columns={"job_status": "Status"})
order_details = order_details[~order_details["Status"].isin(["ABANDONED"])]
order_details = order_details[
    ~order_details["order_id"].isin(
        [
            "734138951872",
            "811738356736",
            "648042957760",
            "239046556928",
            "423486580736",
            "536463465088",
        ]
    )
]
order_details = order_details[
    ~order_details["customer_id"].isin(
        [
            "2059318.0",
            "1506025442.0",
            "1694397201.0",
            "2830181885.0",
            "5620828389.0",
            "4064611739.0",
            "3385745613.0",
            "2281370.0",
            "64438759505.0",
            "569994573568.0",
            "1628682.0",
            "310179181696.0",
            "878446.0",
            "3643707.0",
            "2253354.0",
            "1771017743.0",
            "727840660224.0",
            "2280761953.0",
            "2864429.0",
            "1505970032.0",
            "1517116.0",
            "929482210496.0",
            "5884716233.0",
            "22781605568.0",
            "2794629.0",
            "47201675489.0",
            "6072524763.0",
            "2342577.0",
            "1440074.0",
            "3666483.0",
            "449701472960.0",
            "869120.0",
            "7304625963.0",
            "2214784702.0",
            "869883.0",
            "2851778338.0",
            "3000794.0",
            "1898245261.0",
            "9816298466.0",
            "7021529167.0",
            "3017838801.0",
            "5624710564.0",
            "1584024035.0",
            "2485567.0",
            "2763532338.0",
            "841024809600.0",
            "1739473.0",
            "2183725.0",
            "3788062.0",
            "23400912794.0",
            "150321448192.0",
            "461317394880.0",
            "2208215.0",
            "3669307840.0",
            "610335616576.0",
            "7478577450.0",
            "13153632574.0",
            "2815691755.0",
            "879984.0",
            "3312616.0",
            "548088380288.0",
            "3526036.0",
            "2367635120.0",
            "24957125457.0",
            "459557812544.0",
            "1290757210.0",
            "507345740736.0",
            "2558315057.0",
            "819751.0",
            "407181581440.0",
            "1412707541.0",
            "1419613392.0",
            "4068655.0",
            "303655560704.0",
            "2389210.0",
            "2765139.0",
            "504153462208.0",
            "2100305133.0",
            "653243920384.0",
            "1253878877.0",
            "43255929830.0",
        ]
    )
]
order_details = order_details.rename(columns={"Order Type": "Order_Type"})
order_details.loc[
    (order_details["customer_id"] == "73187559488.0"), "Order_Type"
] = "EXTERNE"
order_details["customer_id"] = order_details["customer_id"].str.rstrip(".0")
order_details["businessCat"] = order_details["businessCat"].replace(
    ["Recharge mobile", "Recharge mobile / ADSL"], ["Airtime", "Airtime"]
)
order_details["customer_origine"] = order_details["paymentType"].apply(
    lambda x: "Diaspora" if x == "CARD_PAY" else "Local"
)
dict_1 = dict(zip(orders["order_id"], orders["transactionId"]))
dict_2 = dict(zip(orders["order_id"], orders["service_fees"]))
order_details["transactionId"] = order_details["order_id"].map(dict_1)
order_details["service_fees"] = order_details["order_id"].map(dict_2)

order_details_pmi = order_details[order_details["Order_Type"] == "EXTERNE"]

# users = pd.read_csv("users.csv", delimiter=",", low_memory=False)
users["customer_id"] = users["customer_id"].astype(str)
users["createdAt"] = pd.to_datetime(users["createdAt"])
# %%
# Filtrer le DataFrame pour ne contenir que les colonnes nécessaires
orders["date"] = pd.to_datetime(orders["date"])

orders = orders[
    [
        "date",
        "Status",
        "customer_origine",
        "paymentType",
        "order_id",
        "Order_Type",
        "businessCat",
        "customer_id",
        "Occurence",
        "previous_order_date",
        "returning_customer",
        "customer_username",
        "customer_phone",
        "customer_email",
        "total_amount_dzd",
    ]
]

orders = orders[orders["businessCat"].notnull()]


# Créer une application Streamlit
# Utilisation du caching pour les filtres et les résultats
@st.cache(allow_output_mutation=True)
def apply_filters(df, status, customer_origine, business_cat, time_period, num_periods):
    filtered_data = df.copy()

    if status != "Tous":
        filtered_data = filtered_data[filtered_data["Status"] == status]

    if customer_origine != "Tous":
        filtered_data = filtered_data[
            filtered_data["customer_origine"] == customer_origine
        ]

    if business_cat != "Toutes":
        filtered_data = filtered_data[filtered_data["businessCat"] == business_cat]

    # Filtrer en fonction de la période sélectionnée
    if time_period == "Semaine":
        date_col = "date"
        period_type = "W"
    else:
        date_col = "date"
        period_type = "M"

    # Calculer la date de début de la période en fonction du nombre de périodes souhaitées
    end_date = filtered_data[date_col].max()
    start_date = end_date - pd.DateOffset(
        months=num_periods
    )  # Change to weeks if needed

    filtered_data = filtered_data[
        (filtered_data[date_col] >= start_date) & (filtered_data[date_col] <= end_date)
    ]

    return filtered_data.copy()


def get_date_range(filtered_data, time_period, num_periods):
    if time_period == "Semaine":
        period_type = "W"
    else:
        period_type = "M"

    end_date = filtered_data["date"].max()
    start_date = end_date - pd.DateOffset(
        months=num_periods
    )  # Change to weeks if needed

    return start_date, end_date


# Créer une application Streamlit
def main():
    global orders
    st.title("Tableau de bord d'analyse de Cohorte")

    # Sidebar pour les filtres
    st.sidebar.title("Filtres")

    # Ajouter une nouvelle section dans la barre latérale pour choisir entre semaines et mois
    time_period = st.sidebar.radio("Période", ["Semaine", "Mois"])

    # Basé sur le choix de l'utilisateur pour la période, créer un nouvel input pour choisir le nombre de semaines ou de mois précédents à afficher

    if time_period == "Semaine":
        num_periods = st.sidebar.number_input("Nombre de dernière Semaines", 1, 36, 4)
    else:
        num_periods = st.sidebar.number_input("Nombre de dernier Mois", 1, 36, 3)

    # Obtenir la valeur sélectionnée par l'utilisateur pour status
    status_options = orders["Status"].unique()
    status = st.sidebar.selectbox("Statut", options=["Tous"] + list(status_options))

    # Obtenir la valeur sélectionnée par l'utilisateur pour customer_origine
    customer_origine = st.sidebar.selectbox(
        "Choississez le type de client (Diaspora ou Local)",
        options=["Tous", "Diaspora", "Local"],
    )

    # Obtenir la valeur sélectionnée par l'utilisateur pour business_cat
    all_business_cats = orders["businessCat"].unique()
    business_cat = st.sidebar.selectbox(
        "Business catégorie", options=["Toutes"] + list(all_business_cats)
    )

    # Utilisation du caching pour les résultats filtrés
    filtered_data = apply_filters(
        orders,
        status,
        customer_origine,
        business_cat,
        time_period,
        num_periods,
    )

    # # Afficher la plage de dates sélectionnée dans l'application
    # start_date, end_date = get_date_range(filtered_data, time_period, num_periods)
    # st.subheader("Plage de dates sélectionnée:")
    # st.write(f"De {start_date.strftime('%Y-%m-%d')} à {end_date.strftime('%Y-%m-%d')}")

    # Créer un bouton de basculement pour afficher/masquer les données filtrées
    show_filtered_data = st.sidebar.checkbox("Afficher les données")

    if show_filtered_data:
        # # Afficher la plage de dates sélectionnée
        # start_date, end_date = get_date_range(filtered_data, time_period, num_periods)
        # st.sidebar.write(
        #     f"Plage de dates sélectionnée : {start_date.strftime('%Y-%m-%d')} à {end_date.strftime('%Y-%m-%d')}"
        # )

        # # Formater les dates pour l'affichage dans le format souhaité
        # filtered_data["Date"] = filtered_data["date"].dt.strftime("%Y-%m-%d")

        # Afficher les résultats dans le contenu de la page
        st.subheader("Data Orders")
        st.dataframe(
            filtered_data
        )  # Utilisation du composant de l'interface utilisateur st.dataframe

        # Boutons de téléchargement pour filtered_data

        st.subheader("Téléchargement de orders")
        download_format = st.radio(
            "Choisir le format de téléchargement :", ["Excel (.xlsx)", "CSV (.csv)"]
        )

        if st.button("Télécharger de données orders"):
            if download_format == "Excel (.xlsx)":
                filtered_data.to_excel("orders.xlsx", index=True)
            elif download_format == "CSV (.csv)":
                filtered_data.to_csv("orders.csv", index=True)

    # Afficher la plage de dates sélectionnée
    start_date, end_date = get_date_range(filtered_data, time_period, num_periods)
    st.sidebar.write(
        f"Plage de dates sélectionnée : {start_date.strftime('%Y-%m-%d')} à {end_date.strftime('%Y-%m-%d')}"
    )

    # Formater les dates pour l'affichage dans le format souhaité
    filtered_data["Date"] = filtered_data["date"].dt.strftime("%Y-%m-%d")

    # st.subheader("Téléchargement de données filtrées")
    # if st.button("Télécharger en Excel (.xlsx)"):
    #     filtered_data.to_excel("filtered_data.xlsx", index=False)
    # if st.button("Télécharger en CSV (.csv)"):
    #     filtered_data.to_csv("filtered_data.csv", index=False)

    # Calculer et afficher l'analyse de cohorte en fonction de la période sélectionnée
    st.subheader("Analyse de Cohorte")
    filtered_data.dropna(subset=["customer_id"], inplace=True)
    filtered_data["date"] = pd.to_datetime(filtered_data["date"])

    # Calculer l'analyse de cohorte en fonction de la période sélectionnée
    if time_period == "Semaine":
        period_frequency = "W"
    else:
        period_frequency = "M"

    filtered_data["order_period"] = filtered_data["date"].dt.to_period(period_frequency)
    # filtered_data['order_month'] = filtered_data['date'].dt.to_period('M')
    filtered_data["cohort"] = (
        filtered_data.groupby("customer_id")["date"]
        .transform("min")
        .dt.to_period(period_frequency)
    )
    filtered_data_cohort = (
        filtered_data.groupby(["cohort", "order_period"])
        .agg(n_customers=("customer_id", "nunique"))
        .reset_index(drop=False)
    )
    filtered_data_cohort["period_number"] = (
        filtered_data_cohort["order_period"] - filtered_data_cohort["cohort"]
    ).apply(attrgetter("n"))

    cohort_pivot = filtered_data_cohort.pivot_table(
        index="cohort", columns="period_number", values="n_customers"
    )

    cohort_size = cohort_pivot.iloc[:, 0]
    retention_matrix = cohort_pivot.divide(cohort_size, axis=0)

    # Afficher la matrice de rétention sous forme de tableau
    st.subheader(
        f"Matrice de Rétention : de {start_date.strftime('%Y-%m-%d')} à {end_date.strftime('%Y-%m-%d')}"
    )
    st.dataframe(retention_matrix)

    st.subheader("Téléchargement de la retention")
    if st.button("Télécharger en Excel (.xlsx)"):
        filtered_data.to_excel(
            f"Matrice de Rétention : de {start_date.strftime('%Y-%m-%d')} à {end_date.strftime('%Y-%m-%d')}.xlsx",
            index=False,
        )

    # Vous pouvez également utiliser une heatmap pour une meilleure visualisation
    st.subheader(
        f"Heatmap de la Matrice de Rétention : de {start_date.strftime('%Y-%m-%d')} à {end_date.strftime('%Y-%m-%d')}"
    )

    # Afficher la heatmap avec Seaborn
    sns.set(style="white")
    plt.figure(figsize=(10, 6))
    sns.heatmap(retention_matrix, annot=True, cmap="YlGnBu", fmt=".0%")
    plt.title(
        f"Heatmap de la Matrice de Rétention : de {start_date.strftime('%Y-%m-%d')} à {end_date.strftime('%Y-%m-%d')}"
    )
    plt.xlabel("Période")
    plt.ylabel("Cohorte")
    st.pyplot(plt)

    # Bouton de téléchargement pour l'image de la heatmap
    if st.button("Télécharger l'image de la Heatmap"):
        # plt.savefig("heatmap.png")
        plt.savefig(
            f"Heatmap de la Matrice de Rétention : de {start_date.strftime('%Y-%m-%d')} à {end_date.strftime('%Y-%m-%d')}.png"
        )
        st.success("Image de la Heatmap téléchargée avec succès !")


# Appel à la fonction main
if __name__ == "__main__":
    main()
