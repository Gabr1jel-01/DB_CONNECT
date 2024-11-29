import customtkinter as ctk
import pyodbc
import pandas as pd
from sqlalchemy import create_engine
import time
from datetime import datetime


server = r"SCADA_POTRESI\WINCC"  # server IP or hostname
database = "IN23_Plant"  # database name
username = "remote_login"  # username is added to the SQL Server logins
password = "1"  # password is for the username that is added in the SQL Server login

query = """SELECT [Current_Time]
      ,[Energy_Total_Reactive_Energy_L1L2L3]
      ,[Energy_Total_Active_Power_L1L2L3]
      ,[Energy_Total_Apparent_Power_L1L2L3]
      ,[Energy_Total_Reactive_Power_L1L2L3]
      ,[Energy_Reset_Total_Active_Energy_L1L2L3]
      ,[Energy_Reset_Total_Reactive_Energy_L1L2L3]
      ,[Energy_Total_Active_Energy_L1L2L3]
      ,[Energy_Current_L1]
      ,[Energy_Current_L2]
      ,[Energy_Current_L3]
      ,[TT_Reactor_Seals_Cooling_Oil]
      ,[TT_Flue_Gas_In]
      ,[TT_Flue_Gas_In_2]
      ,[TT_Flue_Gas_Out]
      ,[TT_Syngas_Afterheater_Out]
      ,[TT_Cyclone_1_Outlet]
      ,[TT_Syngas_Reactor_Out]
      ,[TT_Wet_Scrubber_Middle]
      ,[TT_Flue_Gas_Out_2]
      ,[TT_Flue_Gas_Out_3]
      ,[PT_Hydraulic_Pressure]
      ,[PT_Reactor_Inlet]
      ,[PT_Wet_Scrubber_Cooling]
      ,[PT_Syngas_Reactor_Out]
      ,[PT_Cyclone_1_Outlet]
      ,[PT_Syngas_Afterheater_Out]
      ,[PT_Syngas_Storage_Pressure]
      ,[PT_Flue_Gas_In]
      ,[PT_Active_Carbon_Outlet]
      ,[PT_Active_Carbon_Inlet]
      ,[AT_pH_Meter_Filtering_Tanks]
      ,[LT_Wet_Scrubber_Outlet]
      ,[LT_Wet_Scrubber_Inlet]
      ,[FM_Volume_Flow]
      ,[FM_Mass_Flow]
      ,[FM_Energy_Flow]
      ,[FM_Flow_Velocity]
      ,[FM_Temperature]
      ,[FM_Calorific_Value]
      ,[FM_Pressure]
      ,[Syngas_CH4]
      ,[Syngas_CO]
      ,[Syngas_CO2]
      ,[Syngas_H2]
      ,[Syngas_H2S]
      ,[Syngas_N2]
      ,[Syngas_O2]
      ,[Syngas_Net_Calorific_Value_1]
      ,[Syngas_Net_Calorific_Value_2]
      ,[Syngas_Gross_Calorific_Value]
      ,[Syngas_Temperature]
      ,[Syngas_Sample_Flow]
      ,[Syngas_Pressure]
      ,[Syngas_Mixture_CH4]
      ,[Syngas_Mixture_CO]
      ,[Syngas_Mixture_CO2]
      ,[Syngas_Mixture_H2]
      ,[Syngas_Mixture_H2S]
      ,[Syngas_Mixture_N2]
      ,[Syngas_Mixture_O2]
      ,[Machine_State]
      ,[VSD_COF_State]
      ,[VSD_COF_Freq]
      ,[VSD_CONV_State]
      ,[VSD_CONV_Freq]
      ,[VSD_DP2_State]
      ,[VSD_DP2_Freq]
      ,[VSD_HYD_State]
      ,[VSD_HYD_Freq]
      ,[VSD_RSC_State]
      ,[VSD_RSC_Freq]
      ,[VSD_RSM_State]
      ,[VSD_RSM_Freq]
      ,[VSD_SBF_State]
      ,[VSD_SBF_Freq]
      ,[VSD_SBM_State]
      ,[VSD_SBM_Freq]
      ,[VSD_SBSF_State]
      ,[VSD_SBSF_Freq]
      ,[VSD_SAMP_State]
      ,[VSD_SAMP_Freq]
      ,[M_IN_4_3_09_State]
      ,[M_IN_4_3_10_State]
      ,[M_IN_4_9_01_State]
      ,[M_IN_5_1_02_State]
      ,[M_IN_5_1_04_State]
      ,[M_IN_5_1_31_State]
      ,[M_IN_6_1_01_State]
      ,[M_IN_6_1_02_State]
      ,[M_IN_8_1_01_State]
      ,[M_IN_8_2_03_State]
      ,[M_IN_8_6_01_State]
      ,[M_IN_8_6_02_State]
      ,[Afterburner_State]
      ,[Afterburner_Power_Feedback]
      ,[Feeder_State]
      ,[PID_Pressure_Setpoint]
      ,[PID_Pressure_Status]
      ,[PID_Wet_Scrubber_Setpoint]
      ,[PID_Wet_Scrubber_Status]
      ,[SV_IN_2_2_21_State]
      ,[SV_IN_3_1_21_State]
      ,[SV_IN_3_1_22_State]
      ,[SV_IN_3_3_21_State]
      ,[SV_IN_3_5_21_State]
      ,[SV_IN_3_5_22_State]
      ,[SV_IN_4_3_21_State]
      ,[SV_IN_4_3_60_State]
      ,[SV_IN_6_1_21_State]
      ,[SV_IN_4_3_11_State]
      ,[EMV_IN_5_1_21_State]
      ,[EMV_IN_5_1_22_State]
      ,[EMV_IN_5_1_23_State]
      ,[EMV_IN_5_1_24_State]
      ,[EMV_IN_5_1_29_State]
      ,[FFV_IN_6_1_03_State]
      ,[FFV_IN_6_1_04_State]
      ,[FFV_IN_6_1_05_State]
      ,[FFV_IN_6_1_06_State]
  FROM [IN23_Plant].[dbo].[General_Table]"""
print("Query created")
print()

connection_str = f'mssql+pyodbc://{username}:{
    password}@{server}/{database}?driver=SQL+Server'

starting_time = datetime.now()
starting_time.strftime("%H:%M:%S")
try:
    # Create an SQLAlchemy engine
    engine = create_engine(connection_str)
    print("Connection Successful...")
    time.sleep(1)
    print("Reading Data...")
    # Use pandas to read the SQL query into a DataFrame
    dataframe = pd.read_sql(query, engine)
    # Save the DataFrame to an Excel file
    dataframe.to_csv('table.csv', index=False)
    print("Writing data to a CSV file...")
    print("Data has been successfully written to 'table.csv'!")
    finish_time = datetime.now()
    finish_time.strftime("%H:%M:%S")
    time_of_execution = finish_time - starting_time
    print()
    print("Time it took to finish te reading:", time_of_execution)

except Exception as e:
    print("Error:", e)
