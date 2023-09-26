# %%
import streamlit as st
import pandas as pd
import numpy as np
import seaborn as sns
import matplotlib.pyplot as plt
from operator import attrgetter
from datetime import datetime, timedelta
import os
import boto3
import openpyxl
from io import StringIO
from io import BytesIO
import bcrypt
import xlsxwriter
import re
from easy_exchange_rates import API
from st_files_connection import FilesConnection
import plotly.express as px
import plotly.graph_objects as go


# %%
# Fonction pour télécharger et charger un DataFrame depuis une URL S3
@st.cache_data  # Ajoutez le décorateur de mise en cache
def load_data_s3(bucket_name, file_name):
    response = s3_client.get_object(Bucket=bucket_name, Key=file_name)
    object_content = response["Body"].read().decode("utf-8")
    return pd.read_csv(StringIO(object_content), delimiter=",", low_memory=False)


conn = st.experimental_connection("s3", type=FilesConnection)
orders = conn.read(
    "one-data-lake/csv_database/orders.csv",
    input_format="csv",
    ttl=600,
    low_memory=False,
)
users = conn.read(
    "one-data-lake/csv_database/users_2023.csv",
    input_format="csv",
    ttl=600,
    low_memory=False,
)

# %%
pd.set_option("display.max_columns", None)
pd.set_option("display.precision", 0)

orders["order_id"] = orders["order_id"].astype(str)
orders["customer_id"] = orders["customer_id"].astype(str)
orders["createdAt"] = pd.to_datetime(orders["createdAt"])
orders = orders.rename(columns={"job_status": "Status"})
orders = orders[~orders["Status"].isin(["ABANDONED"])]
orders["customer_id"] = [
    re.sub(r"\.0$", "", customer_id) for customer_id in orders["customer_id"]
]
orders["businessCat"] = orders["businessCat"].replace(
    ["Recharge mobile", "Recharge mobile / ADSL"], ["Airtime", "Airtime"]
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
            "2059318",
            "1506025442",
            "1694397201",
            "2830181885",
            "5620828389",
            "4064611739",
            "3385745613",
            "2281370",
            "64438759505",
            "569994573568",
            "1628682",
            "310179181696",
            "878446",
            "3643707",
            "2253354",
            "1771017743",
            "727840660224",
            "2280761953",
            "2864429",
            "1505970032",
            "1517116",
            "929482210496",
            "5884716233",
            "22781605568",
            "2794629",
            "47201675489",
            "6072524763",
            "2342577",
            "1440074",
            "3666483",
            "449701472960",
            "869120",
            "7304625963",
            "2214784702",
            "869883",
            "2851778338",
            "3000794",
            "1898245261",
            "9816298466",
            "7021529167",
            "3017838801",
            "5624710564",
            "1584024035",
            "2485567",
            "2763532338",
            "841024809600",
            "1739473",
            "2183725",
            "3788062",
            "23400912794",
            "150321448192",
            "461317394880",
            "2208215",
            "3669307840",
            "610335616576",
            "7478577450",
            "13153632574",
            "2815691755",
            "879984",
            "3312616",
            "548088380288",
            "3526036",
            "2367635120",
            "24957125457",
            "459557812544",
            "1290757210",
            "507345740736",
            "2558315057",
            "819751",
            "407181581440",
            "1412707541",
            "1419613392",
            "4068655",
            "303655560704",
            "2389210",
            "2765139",
            "504153462208",
            "2100305133",
            "653243920384",
            "1253878877",
            "43255929830",
        ]
    )
]
orders = orders.rename(columns={"Order Type": "Order_Type"})
orders.loc[(orders["customer_id"] == "73187559488.0"), "Order_Type"] = "EXTERNE"

orders_pmi = orders[orders["Order_Type"] == "EXTERNE"]

users["customer_id"] = users["customer_id"].astype(str)
users["customer_id"] = [
    re.sub(r"\.0$", "", customer_id) for customer_id in users["customer_id"]
]
users["tags"] = users["tags"].str.replace(r"\[|\]", "", regex=True)
users["tags"] = users["tags"].str.replace(r"['\"]", "", regex=True)
users = users[
    ~users["customer_id"].isin(
        [
            "2059318",
            "1506025442",
            "1694397201",
            "2830181885",
            "5620828389",
            "4064611739",
            "3385745613",
            "2281370",
            "64438759505",
            "569994573568",
            "1628682",
            "310179181696",
            "878446",
            "3643707",
            "2253354",
            "1771017743",
            "727840660224",
            "2280761953",
            "2864429",
            "1505970032",
            "1517116",
            "929482210496",
            "5884716233",
            "22781605568",
            "2794629",
            "47201675489",
            "6072524763",
            "2342577",
            "1440074",
            "3666483",
            "449701472960",
            "869120",
            "7304625963",
            "2214784702",
            "869883",
            "2851778338",
            "3000794",
            "1898245261",
            "9816298466",
            "7021529167",
            "3017838801",
            "5624710564",
            "1584024035",
            "2485567",
            "2763532338",
            "841024809600",
            "1739473",
            "2183725",
            "3788062",
            "23400912794",
            "150321448192",
            "461317394880",
            "2208215",
            "3669307840",
            "610335616576",
            "7478577450",
            "13153632574",
            "2815691755",
            "879984",
            "3312616",
            "548088380288",
            "3526036",
            "2367635120",
            "24957125457",
            "459557812544",
            "1290757210",
            "507345740736",
            "2558315057",
            "819751",
            "407181581440",
            "1412707541",
            "1419613392",
            "4068655",
            "303655560704",
            "2389210",
            "2765139",
            "504153462208",
            "2100305133",
            "653243920384",
            "1253878877",
            "43255929830",
        ]
    )
]

users["createdAt"] = pd.to_datetime(users["createdAt"])
users["createdAt"] = users["createdAt"].dt.strftime("%Y-%m-%d")
users["date"] = users["createdAt"]
users["date"] = pd.to_datetime(users["date"])
users = users.rename(columns={"Origine": "customer_origine"})
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

orders_ltv = orders[orders["businessCat"].isin(["Alimentation", "Shopping", "Airtime"])]
# %%
# Créez une base de données utilisateur
# Accédez aux informations de l'utilisateur depuis les secrets
user1_username = st.secrets["st_utilisateurs_1"]["st_username"]
user1_password = st.secrets["st_utilisateurs_1"]["st_password"]

user2_username = st.secrets["st_utilisateurs_2"]["st_username"]
user2_password = st.secrets["st_utilisateurs_2"]["st_password"]

# Créez un dictionnaire user_db avec les informations d'utilisateur hachées
user_db = {
    user1_username: {
        "mot_de_passe": bcrypt.hashpw(user1_password.encode(), bcrypt.gensalt())
    },
    user2_username: {
        "mot_de_passe": bcrypt.hashpw(user2_password.encode(), bcrypt.gensalt())
    },
}


# Fonction de connexion
def login(user_db):
    st.title("Connexion")
    username = st.text_input("Nom d'utilisateur")
    password = st.text_input("Mot de passe", type="password")

    if st.button("Se connecter"):
        if username in user_db:
            hashed_password = user_db[username]["mot_de_passe"]
            if bcrypt.checkpw(password.encode(), hashed_password):
                st.success("Connexion réussie !")
                return True
            else:
                st.error("Nom d'utilisateur ou mot de passe incorrect.")
        else:
            st.error("Nom d'utilisateur non trouvé.")

    return False


def verify_credentials(username, password):
    if username in user_db:
        hashed_password = user_db[username]["mot_de_passe"]
        return bcrypt.checkpw(password.encode(), hashed_password)
    return False


# Fonction pour appliquer les filtres
@st.cache_data
def apply_filters(df, status, customer_origine, business_cat, start_date, end_date):
    filtered_data = df.copy()

    if status != "Tous":
        filtered_data = filtered_data[filtered_data["Status"] == status]

    if customer_origine != "Tous":
        filtered_data = filtered_data[
            filtered_data["customer_origine"] == customer_origine
        ]

    if business_cat != "Toutes":
        filtered_data = filtered_data[filtered_data["businessCat"] == business_cat]

    date_col = "date"

    # Convertir start_date et end_date en datetime64[ns]
    start_date = pd.to_datetime(start_date)
    end_date = pd.to_datetime(end_date)

    # Convertir la colonne de dates en datetime64[ns]
    filtered_data[date_col] = pd.to_datetime(filtered_data[date_col])

    # Filtrer les données en fonction de la plage de dates sélectionnée
    filtered_data = filtered_data[
        (filtered_data[date_col] >= start_date) & (filtered_data[date_col] <= end_date)
    ]

    return filtered_data.copy()


def apply_filters_summary(df, status, customer_origine, start_date, end_date):
    filtered_data = df.copy()

    if status != "Tous":
        filtered_data = filtered_data[filtered_data["Status"] == status]

    if customer_origine != "Tous":
        filtered_data = filtered_data[
            filtered_data["customer_origine"] == customer_origine
        ]

    date_col = "date"

    # Convertir start_date et end_date en datetime64[ns]
    start_date = pd.to_datetime(start_date)
    end_date = pd.to_datetime(end_date)

    # Convertir la colonne de dates en datetime64[ns]
    filtered_data[date_col] = pd.to_datetime(filtered_data[date_col])

    # Filtrer les données en fonction de la plage de dates sélectionnée
    filtered_data = filtered_data[
        (filtered_data[date_col] >= start_date) & (filtered_data[date_col] <= end_date)
    ]

    return filtered_data.copy()


def apply_filters_users(df, customer_origine, customer_country, start_date, end_date):
    filtered_data = df.copy()

    if customer_origine != "Tous":
        filtered_data = filtered_data[
            filtered_data["customer_origine"] == customer_origine
        ]

    if customer_country != "Tous":
        filtered_data = filtered_data[
            filtered_data["customer_country"] == customer_country
        ]

    # if tags != "Tous":
    #     filtered_data = filtered_data[filtered_data["tags"] == tags]

    # if accountTypes != "Tous":
    #     filtered_data = filtered_data[filtered_data["accountTypes"] == accountTypes]

    date_col = "date"

    # Convertir start_date et end_date en datetime64[ns]
    start_date = pd.to_datetime(start_date)
    end_date = pd.to_datetime(end_date)

    # Convertir la colonne de dates en datetime64[ns]
    filtered_data[date_col] = pd.to_datetime(filtered_data[date_col])

    # Filtrer les données en fonction de la plage de dates sélectionnée
    filtered_data = filtered_data[
        (filtered_data[date_col] >= start_date) & (filtered_data[date_col] <= end_date)
    ]

    return filtered_data.copy()


# Créer une application Streamlit
def main():
    st.title("Tableau de Bord TemtemOne")

    # # Zone de connexion
    # if "logged_in" not in st.session_state:
    #     st.session_state.logged_in = False

    # if not st.session_state.logged_in:
    #     st.subheader("Connexion Requise")
    #     username = st.text_input("Nom d'utilisateur")
    #     password = st.text_input("Mot de passe", type="password")

    #     if st.button("Se connecter"):
    #         if verify_credentials(username, password):
    #             st.session_state.logged_in = True
    #         else:
    #             st.error("Nom d'utilisateur ou mot de passe incorrect.")
    #     return

    # Créer un menu de navigation
    selected_page = st.sidebar.selectbox(
        "Sélectionnez un Tableau de Bord",
        ["Retention", "Lifetime Value (LTV)", "Users"],
    )

    ####################################################################################   RETENTION PAGES   #####################################################################

    if selected_page == "Retention":
        # Contenu de la page "Tableau de Bord de Retention"
        st.header("Retention")

        # Sidebar pour les filtres
        st.sidebar.title("Filtres")

        # Sélection manuelle de la date de début
        start_date = st.sidebar.date_input(
            "Date de début", datetime(datetime.now().year, 1, 1).date()
        )
        end_date = st.sidebar.date_input(
            "Date de fin", pd.to_datetime(orders["date"].max()).date()
        )

        # Filtres
        status_options = ["Tous"] + list(orders["Status"].unique())
        status = st.sidebar.selectbox("Statut", status_options)

        customer_origine_options = ["Tous"] + list(orders["customer_origine"].unique())
        customer_origine = st.sidebar.selectbox(
            "Customer Origine (diaspora or Local)", customer_origine_options
        )

        business_cat_options = ["Toutes"] + list(orders["businessCat"].unique())
        business_cat = st.sidebar.selectbox("Business catégorie", business_cat_options)

        # Appliquer les filtres
        filtered_data = apply_filters(
            orders,
            status,
            customer_origine,
            business_cat,
            start_date,
            end_date,
        )

        # Afficher les données filtrées
        show_filtered_data = st.sidebar.checkbox("Afficher les données")

        # Fonction pour convertir un DataFrame en un fichier Excel en mémoire
        def to_excel(df, include_index=True):
            output = BytesIO()
            with pd.ExcelWriter(output, engine="xlsxwriter") as writer:
                df.to_excel(writer, index=include_index, sheet_name="Sheet1")
                workbook = writer.book
                worksheet = writer.sheets["Sheet1"]
                format = workbook.add_format({"num_format": "0.00"})
                worksheet.set_column("A:A", None, format)
            processed_data = output.getvalue()
            return processed_data

        if show_filtered_data:
            st.subheader("Data Orders")
            st.dataframe(filtered_data)

            # Bouton pour télécharger le DataFrame au format Excel
            filtered_data_xlsx = to_excel(filtered_data, include_index=False)
            st.download_button(
                "Télécharger les Orders en Excel (.xlsx)",
                filtered_data_xlsx,
                f"Orders - ORIGINE : {customer_origine} - BUSINESS CATÈGORIE : {business_cat} - STATUS : {status}, du {start_date} au {end_date}.xlsx",
                "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            )

        # Afficher la plage de dates sélectionnée
        st.sidebar.write(f"Plage de dates sélectionnée : du {start_date} au {end_date}")

        # Calculer et afficher l'analyse de cohorte
        st.subheader("Analyse de Cohorte")
        filtered_data.dropna(subset=["customer_id"], inplace=True)
        # filtered_data["date"] = pd.to_datetime(filtered_data["date"])

        filtered_data["order_period"] = filtered_data["date"].dt.to_period("M")
        filtered_data["cohort"] = (
            filtered_data.groupby("customer_id")["date"]
            .transform("min")
            .dt.to_period("M")
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

        # Calculer les clients qui ont abandonné (churn) pour chaque cohort
        churned_customers = cohort_pivot.copy()
        churned_customers.iloc[:, 1:] = (
            cohort_pivot.iloc[:, 1:].values - cohort_pivot.iloc[:, :-1].values
        )
        churned_customers.columns = [
            f"Churn_{col}" for col in churned_customers.columns
        ]

        retention = cohort_pivot.divide(cohort_pivot.iloc[:, 0], axis=0)

        # Créez la heatmap de la matrice de Retention analysis en pourcentage
        retention.index = retention.index.strftime("%Y-%m")
        retention.columns = retention.columns.astype(str)

        # Créez une fonction pour formater les valeurs
        def format_value(x):
            if not pd.isna(x):
                return f"{x:.2f}"
            else:
                return ""

        # Appliquez la fonction pour formater les valeurs dans le DataFrame
        heatmap_data = retention * 100
        heatmap_data = heatmap_data.applymap(format_value)

        # Créez une liste des étiquettes d'axe X (period_number) et d'axe Y (cohort)
        x_labels = heatmap_data.columns.tolist()  # Liste des périodes (0, 1, 2, ...)
        y_labels = (
            heatmap_data.index.tolist()
        )  # Liste des cohortes (2023-01, 2023-02, ...)

        # Créez un graphique en utilisant px.imshow avec les étiquettes X et Y spécifiées
        fig_retention = px.imshow(heatmap_data, x=x_labels, y=y_labels)

        # Personnalisez le texte à afficher pour chaque point de données (gardez deux chiffres après la virgule)
        custom_data = [
            [f"{value:.2f}%" if value is not None else "" for value in row]
            for row in (retention * 100).values
        ]

        # Mettez à jour le texte personnalisé dans le graphique
        fig_retention.update_traces(
            customdata=custom_data, hovertemplate="%{customdata}<extra></extra>"
        )

        # Ajoutez les annotations dans les cases de la heatmap
        for i in range(len(y_labels)):
            for j in range(len(x_labels)):
                value = heatmap_data.iloc[i, j]
                if not pd.isna(value):
                    font_color = (
                        "black" if j == 0 else "white"
                    )  # Noir pour la première colonne, blanc pour les autres
                    fig_retention.add_annotation(
                        text=value,  # Format du texte à afficher
                        x=x_labels[j],
                        y=y_labels[i],
                        showarrow=False,
                        font=dict(color=font_color),  # Couleur du texte
                    )

        # Créez la heatmap de la matrice du nombre de clients
        cohort_pivot.index = cohort_pivot.index.strftime("%Y-%m")
        cohort_pivot.columns = cohort_pivot.columns.astype(str)

        # Créez une liste des étiquettes d'axe X (period_number) et d'axe Y (cohort)
        x_labels = cohort_pivot.columns.tolist()  # Liste des périodes (0, 1, 2, ...)
        y_labels = (
            cohort_pivot.index.tolist()
        )  # Liste des cohortes (2023-01, 2023-02, ...)

        # Créez un graphique en utilisant px.imshow avec les étiquettes X et Y spécifiées
        fig_clients = px.imshow(cohort_pivot, x=x_labels, y=y_labels)

        # Ajoutez les annotations dans les cases de la heatmap
        for i in range(len(y_labels)):
            for j in range(len(x_labels)):
                value = cohort_pivot.iloc[i, j]
                if not pd.isna(value):
                    font_color = (
                        "black" if j == 0 else "white"
                    )  # Noir pour la première colonne, blanc pour les autres
                    fig_clients.add_annotation(
                        text=value,  # Format du texte à afficher
                        x=x_labels[j],
                        y=y_labels[i],
                        showarrow=False,
                        font=dict(color=font_color),  # Couleur du texte
                    )

        # Créez des onglets pour basculer entre les deux visualisations
        selected_visualization = st.radio(
            "Sélectionnez la visualisation", ["Retention Analysis", "Nombre de Clients"]
        )

        if selected_visualization == "Retention Analysis":
            # Affichez la heatmap de l'analyse de rétention
            st.plotly_chart(fig_retention)  # Utilisez le graphique fig_retention
        else:
            # Affichez la heatmap du nombre de clients
            st.plotly_chart(fig_clients)  # Utilisez le graphique fig_clients

        # Téléchargement de la  Rétention
        retention_analysis_xlsx = to_excel(retention, include_index=True)
        st.download_button(
            "Télécharger la Retention analysis en Excel (.xlsx)",
            retention_analysis_xlsx,
            f"Retention analysis - ORIGINE : {customer_origine} - BUSINESS CATÈGORIE : {business_cat} - STATUS : {status}, du {start_date} au {end_date}.xlsx",
            "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        )

        # Téléchargement de la data de Client cohort en excel
        cohort_pivot_xlsx = to_excel(cohort_pivot, include_index=True)
        st.download_button(
            "Télécharger Client cohort en Excel (.xlsx)",
            cohort_pivot_xlsx,
            f"Client cohort - ORIGINE : {customer_origine} - BUSINESS CATÈGORIE : {business_cat} - STATUS : {status}, du {start_date} au {end_date}.xlsx",
            "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        )

    ####################################################################################   LTV PAGES   #####################################################################

    # Créez une nouvelle page LTV
    elif selected_page == "Lifetime Value (LTV)":
        st.header("Lifetime Value (LTV)")

        # Sidebar pour les filtres
        st.sidebar.title("Filtres")

        # Sélection manuelle de la date de début
        start_date = st.sidebar.date_input(
            "Date de début", datetime(datetime.now().year, 1, 1).date()
        )
        end_date = st.sidebar.date_input(
            "Date de fin", pd.to_datetime(orders["date"].max()).date()
        )

        # Filtres
        status_options = ["Tous"] + list(orders["Status"].unique())
        status = st.sidebar.selectbox("Statut", status_options)

        customer_origine_options = ["Tous"] + list(orders["customer_origine"].unique())
        customer_origine = st.sidebar.selectbox(
            "Customer Origine (diaspora or Local)", customer_origine_options
        )

        business_cat_options = ["Toutes"] + list(orders["businessCat"].unique())
        business_cat = st.sidebar.selectbox("Business catégorie", business_cat_options)

        # Appliquer les filtres
        filtered_data_ltv = apply_filters(
            orders_ltv,
            status,
            customer_origine,
            business_cat,
            start_date,
            end_date,
        )

        # Grouper les commandes par 'customer_id' et calculer le nombre de commandes et le montant total dépensé pour chaque client sur les données filtrées
        ltv_df = filtered_data_ltv.groupby("customer_id").agg(
            {"order_id": "count", "total_amount_dzd": "sum", "date": ["min", "max"]}
        )
        ltv_df.columns = [
            "Nombre de commandes",
            "Chiffre d'affaire",
            "min_date",
            "max_date",
        ]
        ltv_df = ltv_df.reset_index()

        # Calculer la durée de vie de chaque client en mois sur les données filtrées
        ltv_df["Durée de vie d’un client (lifetime)"] = (
            ltv_df["max_date"] - ltv_df["min_date"]
        ).dt.days / 30

        # Supprimer les clients ayant une durée de vie nulle (uniquement une commande) sur les données filtrées
        ltv_df = ltv_df[ltv_df["Durée de vie d’un client (lifetime)"] > 0]

        # Diviser le montant total dépensé par le nombre de commandes pour obtenir la valeur moyenne des commandes sur les données filtrées
        ltv_df["Panier moyen"] = (
            ltv_df["Chiffre d'affaire"] / ltv_df["Nombre de commandes"]
        )

        # Diviser le nombre de commandes par la durée de vie de chaque client pour obtenir la fréquence d'achat sur les données filtrées
        ltv_df["Fréquence d’achat"] = (
            ltv_df["Nombre de commandes"]
            / ltv_df["Durée de vie d’un client (lifetime)"]
        )

        # Calculer la LTV en multipliant la fréquence d'achat par la valeur moyenne des commandes et en multipliant le résultat par la durée de vie du client en mois sur les données filtrées
        ltv_df["LTV"] = (
            ltv_df["Fréquence d’achat"]
            * ltv_df["Panier moyen"]
            * ltv_df["Durée de vie d’un client (lifetime)"]
        )

        # Afficher les données filtrées
        show_ltv_df = st.sidebar.checkbox("Afficher les données")

        # Fonction pour convertir un DataFrame en un fichier Excel en mémoire
        def to_excel(df, include_index=True):
            output = BytesIO()
            with pd.ExcelWriter(output, engine="xlsxwriter") as writer:
                df.to_excel(writer, index=include_index, sheet_name="Sheet1")
                workbook = writer.book
                worksheet = writer.sheets["Sheet1"]
                format = workbook.add_format({"num_format": "0.00"})
                worksheet.set_column("A:A", None, format)
            processed_data = output.getvalue()
            return processed_data

        if show_ltv_df:
            st.subheader("LTV Data")
            st.dataframe(ltv_df)

            # Bouton pour télécharger le DataFrame au format Excel
            ltv_df_xlsx = to_excel(ltv_df, include_index=False)
            st.download_button(
                "Télécharger les données de la LTV en Excel (.xlsx)",
                ltv_df_xlsx,
                f"LTV - ORIGINE : {customer_origine} - BUSINESS CATÈGORIE : {business_cat} - STATUS : {status},du {start_date} au {end_date}.xlsx",
                "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            )

        # Appliquer les filtres
        filtered_data_ltv_summary = apply_filters_summary(
            orders_ltv,
            status,
            customer_origine,
            start_date,
            end_date,
        )

        # Afficher la plage de dates sélectionnée
        st.sidebar.write(f"Plage de dates sélectionnée : du {start_date} au {end_date}")

        # Liste unique de catégories d'entreprise
        business_cats = filtered_data_ltv_summary["businessCat"].unique()

        # Initialiser une liste pour stocker les résultats par Business Catégorie
        ltv_results = []

        # Parcourir chaque Business Catégorie et calculer la LTV
        for business_cat in business_cats:
            # Filtrer les données pour la Business Catégorie actuelle
            ltv_cat_df = filtered_data_ltv_summary[
                filtered_data_ltv_summary["businessCat"] == business_cat
            ]

            # Grouper les commandes par 'customer_id' et calculer le nombre de commandes et le montant total dépensé pour chaque client sur les données filtrées
            ltv_df_summary_cat = ltv_cat_df.groupby("customer_id").agg(
                {"order_id": "count", "total_amount_dzd": "sum", "date": ["min", "max"]}
            )

            ltv_df_summary_cat.columns = [
                "Nombre de commandes",
                "Chiffre d'affaire",
                "min_date",
                "max_date",
            ]

            ltv_df_summary_cat = ltv_df_summary_cat.reset_index()

            # Calculer la durée de vie de chaque client en mois sur les données filtrées
            ltv_df_summary_cat["Durée de vie d’un client (lifetime)"] = (
                ltv_df_summary_cat["max_date"] - ltv_df_summary_cat["min_date"]
            ).dt.days / 30

            # Supprimer les clients ayant une durée de vie nulle (uniquement une commande) sur les données filtrées
            ltv_df_summary_cat = ltv_df_summary_cat[
                ltv_df_summary_cat["Durée de vie d’un client (lifetime)"] > 0
            ]

            # Diviser le montant total dépensé par le nombre de commandes pour obtenir la valeur moyenne des commandes sur les données filtrées
            ltv_df_summary_cat["Panier moyen"] = (
                ltv_df_summary_cat["Chiffre d'affaire"]
                / ltv_df_summary_cat["Nombre de commandes"]
            )

            # Diviser le nombre de commandes par la durée de vie de chaque client pour obtenir la fréquence d'achat sur les données filtrées
            ltv_df_summary_cat["Fréquence d’achat"] = (
                ltv_df_summary_cat["Nombre de commandes"]
                / ltv_df_summary_cat["Durée de vie d’un client (lifetime)"]
            )

            # Calculer la LTV en multipliant la fréquence d'achat par la valeur moyenne des commandes et en multipliant le résultat par la durée de vie du client en mois sur les données filtrées
            ltv_df_summary_cat["LTV"] = (
                ltv_df_summary_cat["Fréquence d’achat"]
                * ltv_df_summary_cat["Panier moyen"]
                * ltv_df_summary_cat["Durée de vie d’un client (lifetime)"]
            )

            # Ajouter une colonne "businessCat" pour indiquer la Business Catégorie
            ltv_df_summary_cat["businessCat"] = business_cat

            # Ajouter les résultats au tableau de résultats
            ltv_results.append(ltv_df_summary_cat)

        # Concaténer les résultats de toutes les catégories en un seul DataFrame
        ltv_summary_df = pd.concat(ltv_results, ignore_index=True)

        # Réorganiser les colonnes si nécessaire
        ltv_summary_df = ltv_summary_df[["businessCat", "LTV"]]

        # Renommer la colonne f"LTV ({time_period})" en "LTV (GMV en DZD)"
        ltv_summary_df.rename(columns={"LTV": "LTV (GMV en DZD)"}, inplace=True)

        # Calculer la moyenne de LTV par Business Catégorie
        ltv_avg_gmv_by_cat = (
            ltv_summary_df.groupby("businessCat")["LTV (GMV en DZD)"]
            .mean()
            .reset_index()
        )

        # # Calculer la moyenne de LTV
        # ltv_avg_with_gmv = ltv_summary_df[f"LTV (GMV en DZD)"].mean()

        # Définir un dictionnaire des pourcentages par catégorie
        pourcentages_par_categorie = {
            "Alimentation": 0.10,  # 10%
            "Shopping": 0.08,  # 8%
            "Airtime": 0.21,  # 21%
        }

        # Ajouter la colonne "LTV (Marge en DZD)" en multipliant les valeurs de "LTV (GMV en DZD)" par les pourcentages correspondants
        ltv_avg_gmv_by_cat["LTV (Marge en DZD)"] = ltv_avg_gmv_by_cat.apply(
            lambda row: row["LTV (GMV en DZD)"]
            * pourcentages_par_categorie.get(row["businessCat"], 0),
            axis=1,
        )

        # Renommer la colonne "businessCat" en "Business Catégorie"
        ltv_avg_gmv_by_cat.rename(
            columns={"businessCat": "Business Catégorie"}, inplace=True
        )

        # Ajouter les % à la colonne "Business Catégorie"
        ltv_avg_gmv_by_cat["Business Catégorie"] = ltv_avg_gmv_by_cat[
            "Business Catégorie"
        ].replace(
            ["Alimentation", "Shopping", "Airtime"],
            [
                "Alimentation (10% de marge)",
                "Shopping: (8% de marge)",
                "Airtime: (21% de marge)",
            ],
        )

        # Créez une instance de l'API (assurez-vous d'importer l'API au préalable)
        api = API()

        # Obtenez la date actuelle
        current_date = datetime.now().strftime("%Y-%m-%d")
        # current_date = "2023-09-24"

        # Obtenez le taux de change entre EUR et DZD pour la date actuelle
        exchange_rate_today = api.get_exchange_rates(
            base_currency="EUR",
            start_date=current_date,
            end_date=current_date,
            targets=["DZD"],
        )

        # Obtenez le taux de change EUR/DZD
        eur_to_dzd_rate = exchange_rate_today[current_date]["DZD"]

        # Convertir les colonnes de LTV en €o en divisant par le taux de change
        ltv_avg_gmv_by_cat["LTV (GMV en €)"] = (
            ltv_avg_gmv_by_cat["LTV (GMV en DZD)"] / eur_to_dzd_rate
        )
        ltv_avg_gmv_by_cat["LTV (Marge en €)"] = (
            ltv_avg_gmv_by_cat["LTV (Marge en DZD)"] / eur_to_dzd_rate
        )

        # Arrondir les colonnes "LTV (GMV en DZD)" et "LTV (Marge en DZD)" à zéro décimal
        ltv_avg_gmv_by_cat[
            [
                "LTV (GMV en DZD)",
                "LTV (Marge en DZD)",
                "LTV (GMV en €)",
                "LTV (Marge en €)",
            ]
        ] = ltv_avg_gmv_by_cat[
            [
                "LTV (GMV en DZD)",
                "LTV (Marge en DZD)",
                "LTV (GMV en €)",
                "LTV (Marge en €)",
            ]
        ].round(
            0
        )

        # Convertir les colonnes en types de données entiers
        ltv_avg_gmv_by_cat[
            [
                "LTV (GMV en DZD)",
                "LTV (Marge en DZD)",
                "LTV (GMV en €)",
                "LTV (Marge en €)",
            ]
        ] = ltv_avg_gmv_by_cat[
            [
                "LTV (GMV en DZD)",
                "LTV (Marge en DZD)",
                "LTV (GMV en €)",
                "LTV (Marge en €)",
            ]
        ].astype(
            int
        )

        pd.options.display.float_format = "{:.2f}".format

        # Afficher le tableau de la LTV
        st.subheader("Moyenne de LTV par Business Catégorie (GMV et Marge de la GMV) :")
        st.dataframe(ltv_avg_gmv_by_cat)

        # Téléchargement de la LTV
        ltv_avg_gmv_by_cat_xlsx = to_excel(ltv_avg_gmv_by_cat, include_index=False)
        st.download_button(
            "Télécharger LTV par Business Catégorie (.xlsx)",
            ltv_avg_gmv_by_cat_xlsx,
            f"LTV par Business Catégorie - ORIGINE : {customer_origine} - STATUS : {status}, du {start_date} au {end_date}.xlsx",
            "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        )

        # Créer une fonction pour générer le graphique
        def generate_ltv_graph(df, devise):
            fig = go.Figure()

            if devise == "€":
                columns = ["LTV (GMV en €)", "LTV (Marge en €)"]
                names = ["LTV (GMV en €)", "LTV (Marge en €)"]
            else:
                columns = ["LTV (GMV en DZD)", "LTV (Marge en DZD)"]
                names = ["LTV (GMV en DZD)", "LTV (Marge en DZD)"]

            for col, name in zip(columns, names):
                fig.add_trace(
                    go.Bar(
                        x=df["Business Catégorie"],
                        y=df[col],
                        name=name,
                    )
                )

            fig.update_layout(
                barmode="group",
                title="LTV par Business Catégorie",
                xaxis_title="Business Catégorie",
                yaxis_title="LTV",
                legend_title="Devise",
            )

            return fig

        # Afficher le graphique dans Streamlit
        st.subheader("LTV par Business Catégorie")

        # Sélection de la devise
        selected_devise = st.selectbox("Sélectionnez la devise :", ["€", "DZD"])

        devise = ""

        if selected_devise != devise:
            devise = selected_devise
            st.plotly_chart(generate_ltv_graph(ltv_avg_gmv_by_cat, devise))

    ####################################################################################   USERS PAGES   #####################################################################

    # Créez une nouvelle page Users
    elif selected_page == "Users":
        st.header("Users 2023")

        # Sidebar pour les filtres
        st.sidebar.title("Filtres")

        # Sélection manuelle de la date de début
        start_date = st.sidebar.date_input(
            "Date de début", datetime(datetime.now().year, 1, 1).date()
        )
        end_date = st.sidebar.date_input(
            "Date de fin", pd.to_datetime(orders["date"].max()).date()
        )

        # Filtres
        customer_origine_options = ["Tous"] + list(users["customer_origine"].unique())
        customer_origine = st.sidebar.selectbox(
            "Customer Origine (diaspora or Local)", customer_origine_options
        )

        customer_country_options = ["Tous"] + list(users["customer_country"].unique())
        customer_country = st.sidebar.selectbox(
            "Customer Country", customer_country_options
        )

        # accountTypes_options = ["Tous"] + list(users["accountTypes"].unique())
        # accountTypes = st.sidebar.selectbox("Account Types", accountTypes_options)

        # tags_options = ["Toutes"] + list(users["tags"].unique())
        # tags = st.sidebar.selectbox("Tags", tags_options)

        # Appliquer les filtres
        filtered_data_users = apply_filters_users(
            users,
            customer_origine,
            customer_country,
            # accountTypes,
            # tags,
            start_date,
            end_date,
        )

        # Afficher les données des Users
        show_filtered_data_users = st.sidebar.checkbox("Afficher les données des Users")

        # Fonction pour convertir un DataFrame en un fichier Excel en mémoire
        def to_excel(df, include_index=False):
            output = BytesIO()
            with pd.ExcelWriter(output, engine="xlsxwriter") as writer:
                df.to_excel(writer, index=include_index, sheet_name="Sheet1")
                workbook = writer.book
                worksheet = writer.sheets["Sheet1"]
                format = workbook.add_format({"num_format": "0.00"})
                worksheet.set_column("A:A", None, format)
            processed_data = output.getvalue()
            return processed_data

        if show_filtered_data_users:
            st.subheader("Data Users")
            st.dataframe(filtered_data_users)

            # Bouton pour télécharger le DataFrame au format Excel
            filtered_data_users_xlsx = to_excel(
                filtered_data_users, include_index=False
            )
            st.download_button(
                "Télécharger les Users en Excel (.xlsx)",
                filtered_data_users_xlsx,
                f"USERS - ORIGINE : {customer_origine} - Customer Country : {customer_country}, du {start_date} au {end_date}.xlsx",
                "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            )

        # Sélectionnez les nouveaux inscrits en fonction des filtres déjà appliqués
        new_signups = filtered_data_users
        new_signups = new_signups[
            [
                "date",
                "customer_id",
                "lastName",
                "firstName",
                "phone",
                "email",
                "customer_country",
                "customer_origine",
            ]
        ]

        # Afficher les données des Users
        show_filtered_data_users = st.sidebar.checkbox(
            "Afficher la liste des nouveaux inscrits"
        )

        # Affichez les nouveaux inscrits dans le tableau de bord
        if show_filtered_data_users:
            st.subheader("Nouveaux Inscrits")
            st.dataframe(new_signups)

            # Téléchargement des nouveaux inscrit
            new_signups_xlsx = to_excel(new_signups, include_index=False)
            st.download_button(
                "Télécharger les données des Nouveaux Inscrits (.xlsx)",
                new_signups_xlsx,
                f"Nouveaux Inscrits - ORIGINE : {customer_origine} - Customer Country : {customer_country}, du {start_date} au {end_date}.xlsx",
                "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            )

        orders_users = orders.copy()
        orders_users = orders_users[orders_users["date"] >= "2023-01-01"]
        orders_users = orders_users[
            [
                "date",
                "customer_id",
                "order_id",
                "Status",
                "customer_origine",
                "businessCat",
            ]
        ].rename(columns={"customer_origine": "customer_origine_orders"})

        new_signups_copy = new_signups.copy()
        new_signups_copy = new_signups_copy.rename(
            columns={"date": "registration_date"}
        )
        new_signups_orders = pd.merge(
            orders_users, new_signups_copy, how="inner", on="customer_id"
        )

        # Affichez les orders des nouveaux inscrits dans le tableau de bord

        show_new_signups_orders = st.sidebar.checkbox(
            "Afficher la liste des orders des nouveaux inscrits"
        )

        if show_new_signups_orders:
            st.subheader("Orders des Nouveaux Inscrits")
            st.dataframe(show_new_signups_orders)

            # Téléchargement des nouveaux inscrit
            new_signups_orders_xlsx = to_excel(new_signups_orders, include_index=False)
            st.download_button(
                "Télécharger les données des Orders des Nouveaux Inscrits (.xlsx)",
                new_signups_orders_xlsx,
                f"Nouveaux Inscrits - ORIGINE : {customer_origine} - Customer Country : {customer_country}, du {start_date} au {end_date}.xlsx",
                "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            )

        # Afficher la plage de dates sélectionnée
        st.sidebar.write(f"Plage de dates sélectionnée : du {start_date} au {end_date}")

        # Nombre total de nouveaux inscrits
        total_new_signups = len(new_signups)

        # Nombre de nouveaux inscrits qui ont acheté (Status COMPLETED)
        new_signups_completed = len(
            new_signups_orders[
                new_signups_orders["Status"] == "COMPLETED"
            ].drop_duplicates(subset="customer_id")
        )

        # Nombre de nouveaux inscrits qui ont commandé (en comptant les order_id)
        new_signups_ordered = len(
            new_signups_orders.drop_duplicates(subset="customer_id")
        )

        # Nombre de nouveaux inscrits qui n'ont pas acheté
        new_signups_not_completed = new_signups_ordered - new_signups_completed

        # Nombre de nouveaux inscrits qui n'ont pas du tout commandé
        new_signups_not_ordered = total_new_signups - new_signups_ordered

        # Créez un DataFrame pour les statistiques
        stats_data = pd.DataFrame(
            {
                "Catégorie": [
                    "Nouveaux Inscrit",
                    "Acheté (COMPLETED)",
                    "N'a pas acheté",
                    "Commandé",
                    "N'a pas commandé",
                ],
                "Nombre": [
                    total_new_signups,
                    new_signups_completed,
                    new_signups_not_completed,
                    new_signups_ordered,
                    new_signups_not_ordered,
                ],
            }
        )

        # Sélection de la granularité de la période
        granularity = st.radio(
            "Sélectionnez la période",
            ["Mois", "Semaine", "Jour"],
            key="granularity_users",
        )

        # Créez une nouvelle colonne "period" pour définir les cohortes en fonction de la granularité sélectionnée
        if granularity == "Jour":
            new_signups["period"] = new_signups["date"]
            period_duration = pd.Timedelta(days=1)  # Une journée
        elif granularity == "Semaine":
            new_signups["period"] = new_signups["date"] - pd.to_timedelta(
                (new_signups["date"].dt.dayofweek + 1) % 7, unit="D"
            )
            period_duration = pd.Timedelta(days=7)  # Une semaine
        else:
            new_signups["period"] = new_signups["date"].dt.strftime(
                "%Y-%m"
            )  # Convertir en format "YYYY-MM"
            period_duration = pd.Timedelta(days=31)  # Un mois

        # Agrégez les données par période (jour, semaine ou mois) et comptez le nombre total d'inscriptions pour le mois
        if period_duration:
            new_signups_count = (
                new_signups.groupby("period").size().reset_index(name="count")
            )
        else:
            new_signups_count = (
                new_signups.groupby("period")
                .size()
                .reset_index(name="count")
                .groupby("period")
                .sum()
                .reset_index()
            )

        # Assurez-vous que la dernière période se termine exactement à la fin de la période sélectionnée
        if len(new_signups_count) > 0 and period_duration:
            first_period_start = new_signups_count["period"].min()
            last_period_end = first_period_start + period_duration
            new_signups_count.loc[0, "period"] = last_period_end

        # Créez un graphique montrant le nombre de nouveaux inscrits par période
        if period_duration:
            period_label = f"{granularity}"
        else:
            period_label = "Mois"

        fig_nmb_usr = px.bar(
            new_signups_count,
            x="period",
            y="count",
            title=f"Nombre de Nouveaux Inscrits par {period_label}",
            labels={"period": period_label, "count": "Nombre de Nouveaux Inscrits"},
        ).update_xaxes(categoryorder="total ascending")

        # Créez un graphique à barres horizontal
        fig_stat = px.bar(
            stats_data,
            x="Nombre",
            y="Catégorie",
            orientation="h",
            text="Nombre",
            title=f"Statistiques des Nouveaux Inscrits",
        )

        # Personnalisez le graphique
        fig_stat.update_traces(texttemplate="%{text}", textposition="outside")

        # Créez des onglets pour basculer entre les deux visualisations
        selected_visualization = st.radio(
            "Sélectionnez la visualisation",
            ["Nombre de Nouveaux Inscrits", "Statistiques des Nouveaux Inscrits"],
        )

        if selected_visualization == "Nombre de Nouveaux Inscrits":
            # Affichez la heatmap de l'analyse de rétention
            st.plotly_chart(fig_nmb_usr)  # Utilisez le graphique fig_nmb_usr
        else:
            # Affichez la heatmap du nombre de clients
            st.plotly_chart(fig_stat)  # Utilisez le graphique fig_stat

        # Filtrer les données pour n'inclure que les achats (Status COMPLETED)
        completed_orders = new_signups_orders[
            new_signups_orders["Status"] == "COMPLETED"
        ]

        # Supprimer la colonne "Status" pour avoir des customer_id uniques par businessCat
        completed_orders = completed_orders.drop(columns=["Status"])

        # Grouper les données par "businessCat" et compter le nombre de customer_id uniques
        businesscat_stats_completed = (
            completed_orders.groupby(["businessCat"])
            .agg({"customer_id": "nunique"})
            .reset_index()
        )

        # Créer un graphique à barres empilées
        fig_businesscat_completed = px.bar(
            businesscat_stats_completed,
            x="businessCat",
            y="customer_id",
            title="Nombre de personnes ayant acheté par businessCat",
        )
        fig_businesscat_completed.update_layout(barmode="stack")

        # # Afficher le graphique
        # st.plotly_chart(fig_businesscat_completed)

        # Renommer les valeurs de la colonne "Status" qui ne sont pas "COMPLETED" en "NOT COMPLETED"
        filtered_data_new_signups_orders_copy = new_signups_orders.copy()
        filtered_data_new_signups_orders_copy[
            "Status"
        ] = filtered_data_new_signups_orders_copy["Status"].map(
            lambda x: "NOT COMPLETED" if x != "COMPLETED" else x
        )

        # Filtrer les données pour n'inclure que les statuts "NOT COMPLETED"
        not_completed_orders = filtered_data_new_signups_orders_copy[
            filtered_data_new_signups_orders_copy["Status"] == "NOT COMPLETED"
        ]

        # Grouper les données par "businessCat" et compter le nombre de customer_id uniques
        businesscat_stats_ordered = (
            not_completed_orders.groupby(["businessCat"])
            .agg({"customer_id": "nunique"})
            .reset_index()
        )

        # Créer un graphique à barres empilées
        fig_businesscat_ordered = px.bar(
            businesscat_stats_ordered,
            x="businessCat",
            y="customer_id",
            title="Nombre de personnes n'ayant pas acheté par businessCat",
        )
        fig_businesscat_ordered.update_layout(barmode="stack")

        # # Afficher le graphique
        # st.plotly_chart(fig_businesscat_ordered)

        # Créez des onglets pour basculer entre les deux visualisations
        selected_visualization = st.radio(
            "Sélectionnez la visualisation",
            [
                "Nombre de personnes ayant acheté par businessCat",
                "Nombre de personnes n'ayant pas acheté par businessCat",
            ],
        )

        if selected_visualization == "Nombre de personnes ayant acheté par businessCat":
            # Affichez la heatmap de l'analyse de rétention
            st.plotly_chart(
                fig_businesscat_completed
            )  # Utilisez le graphique fig_nmb_usr
        else:
            # Affichez la heatmap du nombre de clients
            st.plotly_chart(fig_businesscat_ordered)  # Utilisez le graphique fig_stat

    ####################################################################################   CSS STYLE   #####################################################################

    st.markdown(
        """
    <style>
    /* Styles pour la barre de navigation et le contenu principal */
    .css-1cypcdb.eczjsme11,
    .css-1wrcr25 {
        background-color: #ffffff !important; /* Fond blanc */
        border: 1px solid #FF6B05; /* Bordure de 1 pixel avec une couleur orange */
    }

    /* Styles pour le texte en noir */
    .css-k7vsyb h1,
    .css-nahz7x,
    .css-x78sv8,
    .css-q8sbsg,
    .css-1n76uvr.e1f1d6gn0 * {
        color: #000000 !important; /* Texte en noir */
    }

    /* Styles pour les boutons */
    .css-19rxjzo.ef3psqc11 {
        background-color: #ffffff !important; /* Couleur de fond orange par défaut */
        color: #FF6B05 !important; /* Couleur du texte en noir par défaut */
        border: 1px solid #FF6B05; /* Bordure noire de 1 pixel */
    }

    .css-19rxjzo.ef3psqc11:hover {
        background-color: #FF6B05 !important; /* Couleur de fond verte lorsque survolé */
        color: #ffffff !important; /* Couleur du texte en blanc lorsque survolé */
    }

    /* Styles pour le texte à l'intérieur des boutons */
    .css-19rxjzo.ef3psqc11 p {
        font-weight: bold;
        color: #FF6B05 !important;
    }

    .css-19rxjzo.ef3psqc11 p:hover {
        color: #ffffff !important; /* Couleur du texte en blanc lorsque survolé */
        font-weight: bold;
    }

    .e1vs0wn31{
        background-color: #D4D4D4 !important; /*
    }
    </style>
    """,
        unsafe_allow_html=True,
    )


if __name__ == "__main__":
    main()
