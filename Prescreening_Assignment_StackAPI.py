#importing necessary modules
import requests
import pandas as pd
import datetime
import seaborn as sns
import psycopg2
from matplotlib import pyplot as plt
#This line sets the base URL for the StackExchange API.
base_url = 'https://api.stackexchange.com/'

#Sets the number of days to retrieve data from.
Days = 30

#Set the starting and ending pages to retrieve data from.
frompage= 1
topage = 10

#Calculate the Unix timestamp for the start and end dates for the API query.
from_date = int((datetime.datetime.now() - datetime.timedelta(days=Days)).timestamp())
to_date = int(datetime.datetime.now().timestamp())

def get_tags():
    
    #Define the query parameters for the API request, including the date range, order, sort, site, filter, and page size.
    query_params = {
        "fromdate": from_date,
        "todate": to_date,
        "order": "desc",
        "sort": "popular",
        "site": "stackoverflow",
        "filter" : "!bMsg5CXICfJ)0K", # this filter is to remove the collectives values which is list of tags (!9W4ABzRVd)())(!azL-mFb.2Z*D*X)(!bLuioGURDGkGgu)
        "page" : frompage,
        "pagesize" :topage
    }

    #Sends a GET request to the StackExchange API with the specified query parameters.
    response = requests.get(base_url + "2.3/tags", params=query_params)

    #Prints the generated API URL and JSON list for debugging purposes.
    #print(response.url)
    #print(response.json())
    
    #Parse the JSON response from the API and extract the tag names and counts ,then tags and counts are appended to the tags list
    tags = []
    if response.ok:
        for tag in response.json()["items"]:
            tags.append((tag["name"], tag["count"]))
            #print(tag)
    #print(tags)

    return tags

      
# Function to connect to the database and insert data from the DataFrame    
def DB_Insert():
    
    # Call the df_json() function to get the DataFrame with the tags data
    df2 = get_tags()
    
    # Define the database connection details
    DBname = 'PRC'
    host = 'localhost'
    user = 'postgres'
    pwd = 'test@123'
    port = '5432'
        
    
    try:
        # Connect to the database
        con = psycopg2.connect(database=DBname ,user=user,host=host, password=pwd,port = port)
        cursor = con.cursor()
        print("Connection Opened.......")   
        
        # Drop the Stack_Tagcount table if it exists
        cursor.execute('DROP TABLE IF EXISTS Stack_Tagcount')
        
        # Create the Stack_Tagcount table
        cursor.execute("Create table Stack_Tagcount(Tag Varchar,Count numeric);") 
        print("Table Created")
        
        # Insert data from the DataFrame into the table
        insert_script = """ INSERT INTO Stack_Tagcount (Tag, Count) VALUES (%s,%s);"""
        insert_values = df2
        for rec in insert_values:
            cursor.execute(insert_script,rec)
        print("Inserted Data")
               
        # Commit the changes and close the cursor and connection
        con.commit()
        cursor.close()
        con.close()
        print("Connection Closed.......")
        
    except Exception as error:
        print("Connection Error: " , error)


# Function to create a Pandas DataFrame from the tags data
def df_chart():
    
    # Call the get_tags() function to get the tags data
    tagee = get_tags()
    
    # Define the column names for the DataFrame
    column_names = ['Tag','Count']
       
    # Create a DataFrame from the tags data and column names
    df = pd.DataFrame(tagee,columns=column_names)
    
    # Sort the DataFrame by 'Count' column in descending order and get the top 10 tags
    df_sorted = df.sort_values(by='Count', ascending=False)
    top_10 = df_sorted.nlargest(10, 'Count')
    
    # Print the top 10 tags
    #print(top_10)
    
    # Create a bar chart of the top 10 tags
    top_10.plot(kind='bar', x='Tag', y='Count', color='blue')

    # Add title and axis labels to the bar chart
    plt.title('Tag Count')
    plt.xlabel('Tag')
    plt.ylabel('Count')

    # Display the bar chart
    plt.show()
        
    #Return the original DataFrame
    #return df

#Using main function calling other functions 
if __name__ == '__main__':
    
    # Call the DB_Insert() function to connect to the database and insert data        
    DB_Insert()
    df_chart()