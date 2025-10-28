"""
Escriba el codigo que ejecute la accion solicitada.
"""

# pylint: disable=import-outside-toplevel


def clean_campaign_data():
    """
    En esta tarea se le pide que limpie los datos de una campaña de
    marketing realizada por un banco, la cual tiene como fin la
    recolección de datos de clientes para ofrecerls un préstamo.

    La información recolectada se encuentra en la carpeta
    files/input/ en varios archivos csv.zip comprimidos para ahorrar
    espacio en disco.

    Usted debe procesar directamente los archivos comprimidos (sin
    descomprimirlos). Se desea partir la data en tres archivos csv
    (sin comprimir): client.csv, campaign.csv y economics.csv.
    Cada archivo debe tener las columnas indicadas.

    Los tres archivos generados se almacenarán en la carpeta files/output/.

    client.csv:
    - client_id
    - age
    - job: se debe cambiar el "." por "" y el "-" por "_"
    - marital
    - education: se debe cambiar "." por "_" y "unknown" por pd.NA
    - credit_default: convertir a "yes" a 1 y cualquier otro valor a 0
    - mortage: convertir a "yes" a 1 y cualquier otro valor a 0

    campaign.csv:
    - client_id
    - number_contacts
    - contact_duration
    - previous_campaing_contacts
    - previous_outcome: cmabiar "success" por 1, y cualquier otro valor a 0
    - campaign_outcome: cambiar "yes" por 1 y cualquier otro valor a 0
    - last_contact_day: crear un valor con el formato "YYYY-MM-DD",
        combinando los campos "day" y "month" con el año 2022.

    economics.csv:
    - client_id
    - const_price_idx
    - eurobor_three_months



    """
    import pandas as pd
    import zipfile
    from pathlib import Path
    from glob import glob


    Path("files/output").mkdir(parents=True, exist_ok=True)


    input_files = sorted(glob("files/input/*.zip"))


    dataframes = []

    # Procesar cada archivo zip
    for zip_file in input_files:
        with zipfile.ZipFile(zip_file, 'r') as z:
            csv_filename = z.namelist()[0]
            with z.open(csv_filename) as f:
                
                df = pd.read_csv(f)
                dataframes.append(df)


    data = pd.concat(dataframes, ignore_index=True)
    
    # crear client_id desde cero
    data['client_id'] = range(len(data))

    client_df = pd.DataFrame()
    client_df['client_id'] = data['client_id']
    client_df['age'] = data['age']
    
    # limpiar job
    client_df['job'] = data['job'].str.replace('.', '', regex=False).str.replace('-', '_', regex=False)
    
    client_df['marital'] = data['marital']
    
    # limpiar education
    client_df['education'] = data['education'].str.replace('.', '_', regex=False)
    client_df['education'] = client_df['education'].replace('unknown', pd.NA)
    
    # limpiar credit_default - buscar en columnas posibles
    if 'default' in data.columns:
        client_df['credit_default'] = (data['default'] == 'yes').astype(int)
    elif 'credit_default' in data.columns:
        client_df['credit_default'] = (data['credit_default'] == 'yes').astype(int)

    # limpiar mortgage - buscar en columnas posibles
    if 'housing' in data.columns:
        client_df['mortgage'] = (data['housing'] == 'yes').astype(int)
    elif 'mortgage' in data.columns:
        client_df['mortgage'] = (data['mortgage'] == 'yes').astype(int)




    campaign_df = pd.DataFrame()
    campaign_df['client_id'] = data['client_id']
    

    # mapear columnas originales a las esperadas
    if 'campaign' in data.columns:
        campaign_df['number_contacts'] = data['campaign']
    elif 'number_contacts' in data.columns:
        campaign_df['number_contacts'] = data['number_contacts']
    
    if 'duration' in data.columns:
        campaign_df['contact_duration'] = data['duration']
    elif 'contact_duration' in data.columns:
        campaign_df['contact_duration'] = data['contact_duration']
    
    if 'previous' in data.columns:
        campaign_df['previous_campaign_contacts'] = data['previous']
    elif 'previous_campaign_contacts' in data.columns:
        campaign_df['previous_campaign_contacts'] = data['previous_campaign_contacts']
    
    # previous_outcome
    if 'poutcome' in data.columns:
        campaign_df['previous_outcome'] = (data['poutcome'] == 'success').astype(int)
    elif 'previous_outcome' in data.columns:
        campaign_df['previous_outcome'] = (data['previous_outcome'] == 'success').astype(int)
    
    # campaign_outcome
    if 'y' in data.columns:
        campaign_df['campaign_outcome'] = (data['y'] == 'yes').astype(int)
    elif 'campaign_outcome' in data.columns:
        campaign_df['campaign_outcome'] = (data['campaign_outcome'] == 'yes').astype(int)
    
    # last_contact_date
    month_map = {
        'jan': '01', 'feb': '02', 'mar': '03', 'apr': '04',
        'may': '05', 'jun': '06', 'jul': '07', 'aug': '08',
        'sep': '09', 'oct': '10', 'nov': '11', 'dec': '12'
    }
    
    campaign_df['last_contact_date'] = data.apply(
        lambda row: f"2022-{month_map[row['month']]}-{str(row['day']).zfill(2)}",
        axis=1
    )


    economics_df = pd.DataFrame()
    economics_df['client_id'] = data['client_id']
    
    if 'cons.price.idx' in data.columns:
        economics_df['cons_price_idx'] = data['cons.price.idx']
    elif 'cons_price_idx' in data.columns:
        economics_df['cons_price_idx'] = data['cons_price_idx']
    
    if 'euribor3m' in data.columns:
        economics_df['euribor_three_months'] = data['euribor3m']
    elif 'euribor_three_months' in data.columns:
        economics_df['euribor_three_months'] = data['euribor_three_months']

    # guardar los archivos
    client_df.to_csv("files/output/client.csv", index=False)
    campaign_df.to_csv("files/output/campaign.csv", index=False)
    economics_df.to_csv("files/output/economics.csv", index=False)

    return


if __name__ == "__main__":
    clean_campaign_data()