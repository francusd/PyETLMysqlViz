#Librerias Requeridas 
import pandas as pd 
import datetime as dtt
import os 
import re 
import mysql.connector
import configparser
import matplotlib.pyplot as plt 
from bokeh.plotting import figure, show
from bokeh.models import ColumnDataSource



#Definicion de la Clase 
class DBConnectionMysql: 
    
    #Constructor
    def __init__(self):     
       
        config = configparser.ConfigParser()
        config.read('config.ini')
        self.host = config['database']['host']
        self.database_name = config['database']['database_name']
        self.port = config['database'].getint('port')
        self.username = config['database']['username']
        self.pwd = config['database']['password']
        self.current_dir = os.getcwd()
   
    
    
    #Funcion para establecer conexion a la base de datos
    def conexion(self):
        try:
            self.connection = mysql.connector.connect(user=self.username,password=self.pwd)
            #print(self.connection.version)
            print("¡Connection Established!")
            print("Python Example: Configuration Files!")
            
        except mysql.connector.DatabaseError as e:
            print(f"DatabaseError: {e}")
            self.connection.close()
            #cur.close()
        except mysql.connector.IntegrityError as e:
                error_obj, = e.args
                print("Customer ID already exists")
                print("Error Code:", error_obj.code)
                print("Error Full Code:", error_obj.full_code)
                print("Error Message:", error_obj.message)
                self.connection.close()
        finally:
            # Close the connection
            if 'connection' in locals():
                self.connection.close()      
    

#funcion para realizar consulta a la base de datos y visualizar los datos con la librerias Matplotlib
    def db_query(self):
        db_con=self.conexion()
        cur = self.connection.cursor()
            
        x= cur.execute("SELECT gender,count(trip_id)  FROM maestria_analitica.trips_from_2019 group by gender;")    
       
        data = cur.fetchall()
        self.connection.close()          
        df = pd.DataFrame(data, columns =['Gender', 'count_trips'])
        x=['Male','Female']
        filter1 = df[df['Gender'].isin(x)]
        tonos = ['blue' if gender == 'Male' else 'pink' for gender in filter1['Gender']]

        #crea la visualizacion
        plt.bar(filter1['Gender'], filter1['count_trips'], color=tonos)
        plt.xlabel('Gender')
        plt.ylabel('Count of Trips')
        plt.title('Divvy Bikes OpenData: Count of Trips by Gender')
        plt.legend()
        plt.show()



#funcion para realizar consulta a la base de datos y visualizar los datos con la librerias BOKEH
    def db_query2(self):
        db_con=self.conexion()
        cur = self.connection.cursor()
            
        x= cur.execute("SELECT gender,count(trip_id)  FROM maestria_analitica.trips_from_2019 group by gender;")    
       
        data = cur.fetchall()
        self.connection.close()          
        df = pd.DataFrame(data, columns =['Gender', 'count_trips'])
        x=['Male','Female']
        filter1 = df[df['Gender'].isin(x)]
        tonos = ['blue' if gender == 'Male' else 'pink' for gender in filter1['Gender']]
       
        # Create a Bokeh ColumnDataSource from the filtered data
        source = ColumnDataSource(data=dict(
            Gender=filter1['Gender'],
            count_trips=filter1['count_trips'],
            color=tonos
        ))

        # Crea la figura de la visualizacion
        p = figure(x_range=filter1['Gender'], #plot_height=300, plot_width=600,
                title="Divvy Bikes OpenData: Count of Trips by Gender",
                toolbar_location=None, tools="")

        
        # define la visualizacion 
        p.vbar(x='Gender', top='count_trips', width=0.6, color='color', source=source)

        # Add labels and styling
        p.xgrid.grid_line_color = None
        p.y_range.start = 0
        p.xaxis.axis_label = "Gender"
        p.yaxis.axis_label = "Count of Trips"

        # Show the plot
        show(p)



#funcion para crear una vista en el entorno de base de datos
    def create_view(self):
        self.conexion()
        cur = self.connection.cursor()
        # Select the database
        use_db_query = "USE maestria_analitica;"
        cur.execute(use_db_query)

        # SQL command to create a view
        create_view_query = """
        CREATE VIEW trips_by_gender_vw AS
        SELECT gender, COUNT(trip_id) AS trip_count
        FROM maestria_analitica.trips_from_2019
        GROUP BY gender;
        """
        cur.execute(create_view_query)
        self.connection.commit()
        print("Se a creado correctamente vista!")



#  *** Main ***
if __name__ == '__main__':
      obj=DBConnectionMysql()                
      #obj.conexion()
      #obj.db_query()   
      obj.db_query2()