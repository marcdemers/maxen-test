# connecting to the database without using the csv files available in the storage bucket
import influxdb
import pandas as pd
import matplotlib.pyplot as plt
import os

client = influxdb.DataFrameClient(host='35.203.94.147', port=8086, username='marcd', password='maxen2019')
#client = influxdb.InfluxDBClient(host='35.203.80.76', port=8086, username='marcd', password='maxen2019')
#client.create_retention_policy(name='2h_policy', duration='1w',default=True, replication="string")

print(client.get_list_database())
client.switch_database('PlaceBonaventure')

result = client.query('SHOW MEASUREMENTS;') #list tables



os.makedirs("processed_data", exist_ok=True)

if not os.path.exists("processed_data/sensor_value.pt"):
    q = """SELECT * FROM "sensor_value" LIMIT 1000000"""

    res = client.query(q)

    # keep only if pandas Dataframe instance
    keys = [k for k, v in res.items() if not isinstance(v, list)]
    values = [v for _, v in res.items() if not isinstance(v, list)]

    sensor_data = values[0]

    sensor_data.to_parquet("processed_data/sensor_value.pt")

else:
    sensor_data = pd.read_parquet("processed_data/sensor_value.pt")



print(sensor_data.head())
print(sensor_data.columns)

#keep only temperature sensors for now
sensor_data_grouped = sensor_data[sensor_data.sensor_type == 'Temperature']



sensor_data_grouped = sensor_data_grouped.groupby(['sensor_id'])


print(sensor_data_grouped.groups.keys())

sensor_data_grouped = sensor_data_grouped.filter(lambda x: len(x) > 2200)

A = sensor_data_grouped.groupby(['sensor_id']).transform(lambda x: (x - x.mean()) / x.std())
sensor_data_grouped = pd.concat([sensor_data_grouped, A.add_suffix("_transformed")], axis=1)

sensor_data_grouped = sensor_data_grouped[sensor_data_grouped.Name.str.contains("Ventilation")]



print(sensor_data_grouped['sensor_id'].unique())

for idx, i in enumerate(sensor_data_grouped['sensor_id'].unique()):
    if idx == 0:
        ax = sensor_data_grouped[sensor_data_grouped.sensor_id == i]['ActualValue_transformed'].plot()
    elif idx < 10:
        sensor_data_grouped[sensor_data_grouped.sensor_id == i]['ActualValue_transformed'].plot(ax=ax, x='date', y='temperature', title='Temperature')


ax.set(xlabel="date", ylabel="temperature")
plt.show()


print("done")




# """SELECT * FROM "sensor_value" WHERE ("sensor_value" = '1')""")
#sudo service influd start


