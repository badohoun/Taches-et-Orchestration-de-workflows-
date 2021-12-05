from prefect import task , Flow , Parameter  
from prefect.engine.results import LocalResult
from typing import Any, Dict, List
from sklearn.model_selection import train_test_split
import pandas as pd

@task
def load_data(path: str) -> pd.DataFrame: 
    return pd.read_csv(path , sep = ";" , header=0)



@task
def cast_columns(data: pd.DataFrame , remove_col :list) -> pd.DataFrame:
    data = pd.concat([data.iloc[:,0:4] , data.iloc[:,5: ].apply(pd.to_numeric , errors = 'coerce') ] , axis =1)
    final_df = data.drop(columns = remove_col)
    return  final_df

@task
def filterdf_with_missingrate(data:pd.DataFrame ) -> pd.DataFrame:
    missing_rate = data.isna().sum()/data.shape[0]
    firstnaset_columns = list(data.columns[(missing_rate < 0.9)])
    secondnaset_columns = list(data.columns[(missing_rate < 0.88) & (missing_rate > 0.75)])
    filterdf = data[firstnaset_columns + secondnaset_columns]
    return filterdf
@task
def preprocessing(df):
    #df['isna']=(df['Power Min [kW]'].isna()) | (df['Power Avg [kW]'].isna()) #  idée -> échec 
    #df = df.fillna(-999)  
    return df.dropna(axis=0)   

@task(log_stdout=True, target="{date:%a %b %d %H:%M:%S %Y}/{task_name}_output", result = LocalResult(dir='C:\\Users\\inno-demo\\MLOPS\\Maintenance_Predictive_Poc\\baseline\\processed'))
def split_data(data: pd.DataFrame, test_data_ratio: float, random_state:int  ,   classes: list) -> Dict[str, Any]:
    """Task for splitting the vsb data set into training and test
    sets, each split into features and labels.
    """
    
    print(f"Splitting data into training and test sets with ratio {test_data_ratio}")

    X, y = data.drop(columns=classes), data[classes]
    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=test_data_ratio , random_state = random_state)



    # When returning many variables, it is a good practice to give them names:
    return dict(
        train_x=X_train,
        train_y=y_train,
        test_x=X_test,
        test_y=y_test,
    )




#------------------------------------------------------------------------------------------------#
#                                        Create a flow                                           #
#------------------------------------------------------------------------------------------------# 


with Flow("data-engineer") as flow:

    # Define parameters
    path = "allin.csv"
    classes = 'Availability [%]'
    #target_col_cast = 'Timestamp'
    remove_col = list(['Power Plant ID' , 'Power Plant' , 'Timestamp' , 'Farm'])
    #test_data_ratio = 0.2
    test_data_ratio = Parameter("test_data_ratio", default=0.2)
    random_state = 0
    # Define tasks
    data = load_data(path = path)
    cast_col_in_data =  cast_columns(data= data , remove_col = remove_col)
    filter_data = filterdf_with_missingrate(cast_col_in_data)
    preprocess_data = preprocessing(filter_data)
    train_test_dict = split_data(data=preprocess_data, test_data_ratio=test_data_ratio, classes=classes , random_state = random_state)
    
    #dimensional_reduction_dataviz =  dataviz_2D_PCA(preprocess_data)
#flow.visualize()
flow.run()
flow.register(project_name="Vsb Project")
#flow.register(project_name="Maintenance Predictive Poc  Project ")

