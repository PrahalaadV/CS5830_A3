try:
    from datetime import timedelta, datetime
    from airflow import DAG
    from airflow.operators.python_operator import PythonOperator
    from airflow.operators.bash_operator import BashOperator
    from airflow.sensors.filesystem import FileSensor
    import pandas as pd
    import os
    import random
    import requests
    from bs4 import BeautifulSoup
    from zipfile import ZipFile
    import apache_beam as beam
    from airflow.models import Variable
    import numpy as np
    import pandas as pd
    import shutil
    from datetime import datetime, timedelta
    import geopandas as gpd
    from geodatasets import get_path
    import logging
    from ast import literal_eval as make_tuple
    import matplotlib.pyplot as plt
    print("DAG Modules succesfully imported")
except Exception as e:
    print("Error  {} ".format(e))


#################################### TASK 1 ########################################

# function to select which files to download from url
def select_data(*args,**kwargs):
    year = kwargs.get("year","Didn't provide year")
    url = 'https://www.ncei.noaa.gov/data/local-climatological-data/access/{}/'.format(year)
    response = requests.get(url)
    soup = BeautifulSoup(response.content, 'html.parser')
    rows = soup.find("table").find_all("tr")[2:-2]
    
    filenames = kwargs.get("filenames","Didn't provide filenames")
    numfile = kwargs.get("numfile","Didn't provide number of files")

    for i in range(numfile):
        ind = random.randint(0, len(rows))
        data = rows[ind].find_all("td")
        filename=data[0].text
        filenames.append(filename)
    
    kwargs['ti'].xcom_push(key='filenames1', value=filenames)
    return "First task executed"

# function to download data from url
def fetch_data(*args,**kwargs):
    year = kwargs.get("year","Didn't provide year")
    url = 'https://www.ncei.noaa.gov/data/local-climatological-data/access/{}/'.format(year)
    filenames = kwargs['ti'].xcom_pull(task_ids='select_data', key='filenames1')
    for filename in filenames:
        newUrl = url+filename
        response = requests.get(newUrl)
        open(filename,'wb').write(response.content) 
    kwargs['ti'].xcom_push(key='filenames2', value=filenames)
    return "Second task executed"

# function to zip files into weather.zip
def zip_data(*args,**kwargs):
    filenames = kwargs['ti'].xcom_pull(task_ids='fetch_data', key='filenames2')
    with ZipFile('weather.zip','w') as zip:
        for filename in filenames:
            zip.write(filename)
    print(zip.printdir()) 
    for filename in filenames:
        os.remove(filename)
    return "Third task executed"

dag_1 = DAG(
    dag_id= "webscrape",
    schedule_interval="@daily",
    default_args={
            "owner": "first_task",
            "retries": 1,
            "retry_delay": timedelta(minutes=5),
            "start_date": datetime(2021, 1, 1),
        },
    catchup=False
)

select_data = PythonOperator(
    task_id="select_data",
    python_callable=select_data,
    provide_context=True,
    op_kwargs = {"name": "first_task", "year": 2023, "numfile": 10, "filenames": []},
    dag=dag_1
)

fetch_data = PythonOperator(
    task_id="fetch_data",
    python_callable=fetch_data,
    provide_context=True,
    op_kwargs={"name":"second_task","year":2023},
    dag=dag_1
)

zip_data = PythonOperator(
    task_id="zip_data",
    python_callable=zip_data,
    provide_context=True,
    op_kwargs={"name":"third_task"},
    dag=dag_1
)

select_data >> fetch_data >> zip_data 


############################## TASK 2 ###############################################

# Function to unzip previously zipped files
def unzip(**context):
   with ZipFile("/home/pvv/bdl/airflow/dags/weather.zip", 'r') as zObject: 
      zObject.extractall(path="/home/pvv/bdl/airflow/dags/weather") 
      
# Function to parse csv files
def parseCSV(data):
    df = data.split('","')
    df[0] = df[0].strip('"')
    df[-1] = df[-1].strip('"')
    return list(df)

class ExtractAndFilterFields(beam.DoFn):
    def __init__(self,required_fields,**kwargs):
        super().__init__(**kwargs)
        self.required_fields = []
        headers = ["STATION","DATE","LATITUDE","LONGITUDE","ELEVATION","NAME","REPORT_TYPE","SOURCE","HourlyAltimeterSetting","HourlyDewPointTemperature","HourlyDryBulbTemperature","HourlyPrecipitation","HourlyPresentWeatherType","HourlyPressureChange","HourlyPressureTendency","HourlyRelativeHumidity","HourlySkyConditions","HourlySeaLevelPressure","HourlyStationPressure","HourlyVisibility","HourlyWetBulbTemperature","HourlyWindDirection","HourlyWindGustSpeed","HourlyWindSpeed","Sunrise","Sunset","DailyAverageDewPointTemperature","DailyAverageDryBulbTemperature","DailyAverageRelativeHumidity","DailyAverageSeaLevelPressure","DailyAverageStationPressure","DailyAverageWetBulbTemperature","DailyAverageWindSpeed","DailyCoolingDegreeDays","DailyDepartureFromNormalAverageTemperature","DailyHeatingDegreeDays","DailyMaximumDryBulbTemperature","DailyMinimumDryBulbTemperature","DailyPeakWindDirection","DailyPeakWindSpeed","DailyPrecipitation","DailySnowDepth","DailySnowfall","DailySustainedWindDirection","DailySustainedWindSpeed","DailyWeather","MonthlyAverageRH","MonthlyDaysWithGT001Precip","MonthlyDaysWithGT010Precip","MonthlyDaysWithGT32Temp","MonthlyDaysWithGT90Temp","MonthlyDaysWithLT0Temp","MonthlyDaysWithLT32Temp","MonthlyDepartureFromNormalAverageTemperature","MonthlyDepartureFromNormalCoolingDegreeDays","MonthlyDepartureFromNormalHeatingDegreeDays","MonthlyDepartureFromNormalMaximumTemperature","MonthlyDepartureFromNormalMinimumTemperature","MonthlyDepartureFromNormalPrecipitation","MonthlyDewpointTemperature","MonthlyGreatestPrecip","MonthlyGreatestPrecipDate","MonthlyGreatestSnowDepth","MonthlyGreatestSnowDepthDate","MonthlyGreatestSnowfall","MonthlyGreatestSnowfallDate","MonthlyMaxSeaLevelPressureValue","MonthlyMaxSeaLevelPressureValueDate","MonthlyMaxSeaLevelPressureValueTime","MonthlyMaximumTemperature","MonthlyMeanTemperature","MonthlyMinSeaLevelPressureValue","MonthlyMinSeaLevelPressureValueDate","MonthlyMinSeaLevelPressureValueTime","MonthlyMinimumTemperature","MonthlySeaLevelPressure","MonthlyStationPressure","MonthlyTotalLiquidPrecipitation","MonthlyTotalSnowfall","MonthlyWetBulb","AWND","CDSD","CLDD","DSNW","HDSD","HTDD","DYTS","DYHF","NormalsCoolingDegreeDay","NormalsHeatingDegreeDay","ShortDurationEndDate005","ShortDurationEndDate010","ShortDurationEndDate015","ShortDurationEndDate020","ShortDurationEndDate030","ShortDurationEndDate045","ShortDurationEndDate060","ShortDurationEndDate080","ShortDurationEndDate100","ShortDurationEndDate120","ShortDurationEndDate150","ShortDurationEndDate180","ShortDurationPrecipitationValue005","ShortDurationPrecipitationValue010","ShortDurationPrecipitationValue015","ShortDurationPrecipitationValue020","ShortDurationPrecipitationValue030","ShortDurationPrecipitationValue045","ShortDurationPrecipitationValue060","ShortDurationPrecipitationValue080","ShortDurationPrecipitationValue100","ShortDurationPrecipitationValue120","ShortDurationPrecipitationValue150","ShortDurationPrecipitationValue180","REM","BackupDirection","BackupDistance","BackupDistanceUnit","BackupElements","BackupElevation","BackupEquipment","BackupLatitude","BackupLongitude","BackupName","WindEquipmentChangeDate"]
        for ind,i in enumerate(headers):
            if 'hourly' in i.lower():
                for j in required_fields:
                    if j.lower() in i.lower():
                        self.required_fields.append(ind)
        self.headers = {i:ind for ind,i in enumerate(headers)} 


    # Function to extract the required fields from the csv files
    def process(self, element):
        headers = self.headers 
        lat = element[headers['LATITUDE']]
        lon = element[headers['LONGITUDE']]
        data = []
        for i in self.required_fields:
            data.append(element[i])
        if lat != 'LATITUDE':
            yield ((lat, lon), data)


# Using above functions in a pipeline using beam
def process_csv(**kwargs):
    required_fields = ["LATITUDE", "LONGITUDE", "HourlyWindSpeed"]
    output_path = '/home/pvv/bdl/airflow/dags/weather/results'

    os.makedirs(output_path, exist_ok=True)

    with beam.Pipeline(runner='DirectRunner') as p:
        result = (
            p
            | 'ReadCSV' >> beam.io.ReadFromText('/home/pvv/bdl/airflow/dags/weather/*.csv')
            | 'ParseData' >> beam.Map(parseCSV)
            | 'FilterAndCreateTuple' >> beam.ParDo(ExtractAndFilterFields(required_fields=required_fields))
            | 'CombineTuple' >> beam.GroupByKey()
            | 'UnpackTuple' >> beam.Map(lambda a: (a[0][0], a[0][1], a[1]))
        )

        result | 'WriteToText' >> beam.io.WriteToText(output_path + '/result.txt')
        

class ExtractFieldsWithMonth(beam.DoFn):
    def __init__(self,required_fields,**kwargs):
        super().__init__(**kwargs)
        self.required_fields = []
        headers = ["STATION","DATE","LATITUDE","LONGITUDE","ELEVATION","NAME","REPORT_TYPE","SOURCE","HourlyAltimeterSetting","HourlyDewPointTemperature","HourlyDryBulbTemperature","HourlyPrecipitation","HourlyPresentWeatherType","HourlyPressureChange","HourlyPressureTendency","HourlyRelativeHumidity","HourlySkyConditions","HourlySeaLevelPressure","HourlyStationPressure","HourlyVisibility","HourlyWetBulbTemperature","HourlyWindDirection","HourlyWindGustSpeed","HourlyWindSpeed","Sunrise","Sunset","DailyAverageDewPointTemperature","DailyAverageDryBulbTemperature","DailyAverageRelativeHumidity","DailyAverageSeaLevelPressure","DailyAverageStationPressure","DailyAverageWetBulbTemperature","DailyAverageWindSpeed","DailyCoolingDegreeDays","DailyDepartureFromNormalAverageTemperature","DailyHeatingDegreeDays","DailyMaximumDryBulbTemperature","DailyMinimumDryBulbTemperature","DailyPeakWindDirection","DailyPeakWindSpeed","DailyPrecipitation","DailySnowDepth","DailySnowfall","DailySustainedWindDirection","DailySustainedWindSpeed","DailyWeather","MonthlyAverageRH","MonthlyDaysWithGT001Precip","MonthlyDaysWithGT010Precip","MonthlyDaysWithGT32Temp","MonthlyDaysWithGT90Temp","MonthlyDaysWithLT0Temp","MonthlyDaysWithLT32Temp","MonthlyDepartureFromNormalAverageTemperature","MonthlyDepartureFromNormalCoolingDegreeDays","MonthlyDepartureFromNormalHeatingDegreeDays","MonthlyDepartureFromNormalMaximumTemperature","MonthlyDepartureFromNormalMinimumTemperature","MonthlyDepartureFromNormalPrecipitation","MonthlyDewpointTemperature","MonthlyGreatestPrecip","MonthlyGreatestPrecipDate","MonthlyGreatestSnowDepth","MonthlyGreatestSnowDepthDate","MonthlyGreatestSnowfall","MonthlyGreatestSnowfallDate","MonthlyMaxSeaLevelPressureValue","MonthlyMaxSeaLevelPressureValueDate","MonthlyMaxSeaLevelPressureValueTime","MonthlyMaximumTemperature","MonthlyMeanTemperature","MonthlyMinSeaLevelPressureValue","MonthlyMinSeaLevelPressureValueDate","MonthlyMinSeaLevelPressureValueTime","MonthlyMinimumTemperature","MonthlySeaLevelPressure","MonthlyStationPressure","MonthlyTotalLiquidPrecipitation","MonthlyTotalSnowfall","MonthlyWetBulb","AWND","CDSD","CLDD","DSNW","HDSD","HTDD","DYTS","DYHF","NormalsCoolingDegreeDay","NormalsHeatingDegreeDay","ShortDurationEndDate005","ShortDurationEndDate010","ShortDurationEndDate015","ShortDurationEndDate020","ShortDurationEndDate030","ShortDurationEndDate045","ShortDurationEndDate060","ShortDurationEndDate080","ShortDurationEndDate100","ShortDurationEndDate120","ShortDurationEndDate150","ShortDurationEndDate180","ShortDurationPrecipitationValue005","ShortDurationPrecipitationValue010","ShortDurationPrecipitationValue015","ShortDurationPrecipitationValue020","ShortDurationPrecipitationValue030","ShortDurationPrecipitationValue045","ShortDurationPrecipitationValue060","ShortDurationPrecipitationValue080","ShortDurationPrecipitationValue100","ShortDurationPrecipitationValue120","ShortDurationPrecipitationValue150","ShortDurationPrecipitationValue180","REM","BackupDirection","BackupDistance","BackupDistanceUnit","BackupElements","BackupElevation","BackupEquipment","BackupLatitude","BackupLongitude","BackupName","WindEquipmentChangeDate"]
        for ind,i in enumerate(headers):
            if 'hourly' in i.lower():
                for j in required_fields:
                    if j.lower() in i.lower():
                        self.required_fields.append(ind)

        self.headers = {i:ind for ind,i in enumerate(headers)}

    ## Function to extract data for each month
    def process(self, element):
        headers = self.headers 
        lat = element[headers['LATITUDE']]
        lon = element[headers['LONGITUDE']]
        data = []
        for i in self.required_fields:
            data.append(element[i])
        if lat != 'LATITUDE':
            Measuretime = datetime.strptime(element[headers['DATE']],'%Y-%m-%dT%H:%M:%S')
            Month_format = "%Y-%m"
            Month = Measuretime.strftime(Month_format)
            yield ((Month, lat, lon), data)


## Function to calculate averages
def compute_avg(data):
    val_data = np.array(data[1])
    val_data_shape = val_data.shape
    val_data = pd.to_numeric(val_data.flatten(), errors='coerce',downcast='float') 
    val_data = np.reshape(val_data,val_data_shape)
    masked_data = np.ma.masked_array(val_data, np.isnan(val_data))
    result = np.ma.average(masked_data, axis=0)
    result = list(res.filled(np.nan))
    logger = logging.getLogger(__name__)
    logger.info(res)
    return ((data[0][1],data[0][2]),result)


# Beam pipeline to compute monthly averages
def compute_monthly_avg( **kwargs):
    required_fields = ["LATITUDE","LONGITUDE","HourlyWindSpeed"]
    os.makedirs('/home/pvv/bdl/airflow/dags/weather/results', exist_ok=True)
    with beam.Pipeline(runner='DirectRunner') as p:
        result = (
            p
            | 'ReadProcessedData' >> beam.io.ReadFromText('/home/pvv/bdl/airflow/dags/weather/*.csv')
            | 'ParseData' >> beam.Map(parseCSV)
            | 'CreateTupleWithMonthInKey' >> beam.ParDo(ExtractFieldsWithMonth(required_fields=required_fields))
            | 'CombineTupleMonthly' >> beam.GroupByKey()
            | 'ComputeAverages' >> beam.Map(lambda data: compute_avg(data))
            | 'CombineTuplewithAverages' >> beam.GroupByKey()
            | 'UnpackTuple' >> beam.Map(lambda a:(a[0][0],a[0][1],a[1]))
        )
        result | 'WriteAveragesToText' >> beam.io.WriteToText('/home/pvv/bdl/airflow/dags/weather/results/averages.txt')
        

class Aggregated(beam.CombineFn):
    def __init__(self,required_fields,**kwargs):
        super().__init__(**kwargs)
        self.required_fields = []
        headers_csv = ["STATION","DATE","LATITUDE","LONGITUDE","ELEVATION","NAME","REPORT_TYPE","SOURCE","HourlyAltimeterSetting","HourlyDewPointTemperature","HourlyDryBulbTemperature","HourlyPrecipitation","HourlyPresentWeatherType","HourlyPressureChange","HourlyPressureTendency","HourlyRelativeHumidity","HourlySkyConditions","HourlySeaLevelPressure","HourlyStationPressure","HourlyVisibility","HourlyWetBulbTemperature","HourlyWindDirection","HourlyWindGustSpeed","HourlyWindSpeed","Sunrise","Sunset","DailyAverageDewPointTemperature","DailyAverageDryBulbTemperature","DailyAverageRelativeHumidity","DailyAverageSeaLevelPressure","DailyAverageStationPressure","DailyAverageWetBulbTemperature","DailyAverageWindSpeed","DailyCoolingDegreeDays","DailyDepartureFromNormalAverageTemperature","DailyHeatingDegreeDays","DailyMaximumDryBulbTemperature","DailyMinimumDryBulbTemperature","DailyPeakWindDirection","DailyPeakWindSpeed","DailyPrecipitation","DailySnowDepth","DailySnowfall","DailySustainedWindDirection","DailySustainedWindSpeed","DailyWeather","MonthlyAverageRH","MonthlyDaysWithGT001Precip","MonthlyDaysWithGT010Precip","MonthlyDaysWithGT32Temp","MonthlyDaysWithGT90Temp","MonthlyDaysWithLT0Temp","MonthlyDaysWithLT32Temp","MonthlyDepartureFromNormalAverageTemperature","MonthlyDepartureFromNormalCoolingDegreeDays","MonthlyDepartureFromNormalHeatingDegreeDays","MonthlyDepartureFromNormalMaximumTemperature","MonthlyDepartureFromNormalMinimumTemperature","MonthlyDepartureFromNormalPrecipitation","MonthlyDewpointTemperature","MonthlyGreatestPrecip","MonthlyGreatestPrecipDate","MonthlyGreatestSnowDepth","MonthlyGreatestSnowDepthDate","MonthlyGreatestSnowfall","MonthlyGreatestSnowfallDate","MonthlyMaxSeaLevelPressureValue","MonthlyMaxSeaLevelPressureValueDate","MonthlyMaxSeaLevelPressureValueTime","MonthlyMaximumTemperature","MonthlyMeanTemperature","MonthlyMinSeaLevelPressureValue","MonthlyMinSeaLevelPressureValueDate","MonthlyMinSeaLevelPressureValueTime","MonthlyMinimumTemperature","MonthlySeaLevelPressure","MonthlyStationPressure","MonthlyTotalLiquidPrecipitation","MonthlyTotalSnowfall","MonthlyWetBulb","AWND","CDSD","CLDD","DSNW","HDSD","HTDD","DYTS","DYHF","NormalsCoolingDegreeDay","NormalsHeatingDegreeDay","ShortDurationEndDate005","ShortDurationEndDate010","ShortDurationEndDate015","ShortDurationEndDate020","ShortDurationEndDate030","ShortDurationEndDate045","ShortDurationEndDate060","ShortDurationEndDate080","ShortDurationEndDate100","ShortDurationEndDate120","ShortDurationEndDate150","ShortDurationEndDate180","ShortDurationPrecipitationValue005","ShortDurationPrecipitationValue010","ShortDurationPrecipitationValue015","ShortDurationPrecipitationValue020","ShortDurationPrecipitationValue030","ShortDurationPrecipitationValue045","ShortDurationPrecipitationValue060","ShortDurationPrecipitationValue080","ShortDurationPrecipitationValue100","ShortDurationPrecipitationValue120","ShortDurationPrecipitationValue150","ShortDurationPrecipitationValue180","REM","BackupDirection","BackupDistance","BackupDistanceUnit","BackupElements","BackupElevation","BackupEquipment","BackupLatitude","BackupLongitude","BackupName","WindEquipmentChangeDate"]
        for i in headers_csv:
            if 'hourly' in i.lower():
                for j in required_fields:
                    if j.lower() in i.lower():
                        self.required_fields.append(i.replace('Hourly',''))

    def create_acc(self):
        return []
    
    def add_input(self, acc, ele):
        acc2 = {key:value for key,value in acc}
        data = ele[2]
        val_data = np.array(data)
        val_data_shape = val_data.shape
        val_data = pd.to_numeric(val_data.flatten(), errors='coerce',downcast='float')
        val_data = np.reshape(val_data,val_data_shape)
        masked_data = np.ma.masked_array(val_data, np.isnan(val_data))
        res = np.ma.average(masked_data, axis=0)
        res = list(res.filled(np.nan))
        for ind,i in enumerate(self.required_fields):
            acc2[i] = acc2.get(i,[]) + [(ele[0],ele[1],res[ind])]

        return list(acc2.items())
    
    def merge_accu(self, accs):
        merged = {}
        for a in accs:
                a2 = {key:value for key,value in a}
                for i in self.required_fields:
                    merged[i] = merged.get(i,[]) + a2.get(i,[])

        return list(merged.items())
    
    def extract_output(self, acc):
        return acc
    

# Plot geomaps
def plot_geomaps(values):
    logger = logging.getLogger(__name__)
    logger.info(values)
    data = np.array(values[1],dtype='float')
    d1 = np.array(data,dtype='float')

    data = gpd.GeoDataFrame({
        values[0]:d1[:,2]
    }, geometry=gpd.points_from_xy(*d1[:,(1,0)].T))
    
    _ , ax = plt.subplots(1, 1, figsize=(10, 5))
    
    data.plot(column=values[0], cmap='viridis', marker='o', markersize=150, ax=ax, legend=True)
    ax.set_title(f'{values[0]} Heatmap')
    os.makedirs('/home/pvv/bdl/airflow/dags/weather/results/plots', exist_ok=True)
    
    plt.savefig(f'/home/pvv/bdl/airflow/dags/weather/results/plots{values[0]}_heatmap_plot.png')


# Pipeline to create plot using Beam
def create_heatmap(**kwargs):
    
    required_fields = ["LATITUDE","LONGITUDE","HourlyWindSpeed"]
    with beam.Pipeline(runner='DirectRunner') as p:
        
        result = (
            p
            | 'ReadProcessedData' >> beam.io.ReadFromText('/home/pvv/bdl/airflow/dags/weather/results/averages.txt*')
            | 'preprocessParse' >>  beam.Map(lambda a:make_tuple(a.replace('nan', 'None')))
            | 'Global aggregation' >> beam.CombineGlobally(Aggregated(required_fields = required_fields))
            | 'Flat Map' >> beam.FlatMap(lambda a:a) 
            | 'Plot Geomaps' >> beam.Map(plot_geomaps)            
        )
        

## Function to delete csv files after completing tasks
def delete_csv(**kwargs):
    shutil.rmtree('/home/pvv/bdl/airflow/dags/weather')


dag_2 = DAG(
    dag_id= "data_analysis",
    schedule_interval="@daily",
    default_args={
            "owner": "first_task",
            "retries": 1,
            "retry_delay": timedelta(minutes=5),
            "start_date": datetime(2021, 1, 1),
        },
    catchup=False
)


wait = FileSensor(
    task_id = 'wait',
    mode="poke",
    poke_interval = 5,  # Check every 5 seconds
    timeout = 5,  # Timeout after 5 seconds
    filepath = "/home/pvv/bdl/airflow/dags/weather.zip",
    dag=dag_2,
    fs_conn_id = "fs_default", # File path system must be defined
)

unzip_files = PythonOperator(
        task_id="unzip_files",
        python_callable=unzip,
        provide_context=True,
        dag=dag_2
    )

process_csv_files = PythonOperator(
    task_id='process_csv_files',
    python_callable=process_csv,
    dag=dag_2,
)

compute_monthly_avgs = PythonOperator(
    task_id='compute_monthly_avgs',
    python_callable=compute_monthly_avg,
    dag=dag_2,
)

plot_heatmap = PythonOperator(
    task_id='create_heatmap',
    python_callable=create_heatmap,
    dag=dag_2,
)

delete_files = PythonOperator(
    task_id='delete_files',
    python_callable=delete_csv,
    dag=dag_2,
)

wait >> unzip_files >> process_csv_files >> compute_monthly_avgs >> plot_heatmap >> delete_files