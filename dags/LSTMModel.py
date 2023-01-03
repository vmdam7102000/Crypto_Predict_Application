import numpy as np
import pandas as pd
from sklearn.preprocessing import MinMaxScaler
from tensorflow.keras.models import Sequential
from tensorflow.keras.layers import LSTM, Dense, Dropout, InputLayer
from tensorflow.keras.callbacks import Callback, EarlyStopping, ModelCheckpoint,ReduceLROnPlateau,TensorBoard
from tensorflow.keras.optimizers import Adam
from tensorflow.keras.wrappers.scikit_learn import KerasRegressor
from sklearn.ensemble import AdaBoostRegressor
import psycopg2.extras as extras
import psycopg2
from airflow.providers.postgres.hooks.postgres import PostgresHook 
import pickle
import os


hook = PostgresHook(postgres_conn_id= "postgres_con")
conn = hook.get_conn()

def execute_values(conn, df, table):
  
    tuples = [tuple(x) for x in df.to_numpy()]
  
    cols = ','.join(list(df.columns))
    # SQL query to execute
    query = "INSERT INTO %s(%s) VALUES %%s" % (table, cols)
    cursor = conn.cursor()
    try:
        extras.execute_values(cursor, query, tuples)
        conn.commit()
    except (Exception, psycopg2.DatabaseError) as error:
        print("Error: %s" % error)
        conn.rollback()
        cursor.close()
        return 1
    print("the dataframe is inserted")
    cursor.close()



def train_model():
    hook = PostgresHook(postgres_conn_id= "postgres_con")
    conn = hook.get_conn()
    df = hook.get_pandas_df(sql="select A.date, A.open, A.high, A.low, A.close, A.volume, B.nlp_compound, B.nlp_subjectivity, B.nlp_polarity from trading_data A left join twitter_sentiment B on A.date = B.date")
    df[['nlp_compound','nlp_subjectivity','nlp_polarity']]=df[['nlp_compound','nlp_subjectivity','nlp_polarity']].fillna(0)
    df.set_index('date',drop=True,inplace=True)
    df.sort_index(inplace=True)


    #trading + sentiment
    col_trading_twitter = ['open','high','low','volume','nlp_compound','nlp_subjectivity','nlp_polarity','close']
    df_NLP = pd.DataFrame(df,columns=col_trading_twitter)

    #init df for model training
    df_train = df_NLP
    scaler = MinMaxScaler()
    df_train_scaled = scaler.fit_transform(df_train)


    scaler_close_price = MinMaxScaler() # Used to inverse_transform close price prediction data
    train_close_price = scaler_close_price.fit(df_train.close.values.reshape(-1,1))


    # Transforms the original time series into the input formar required by the LSTM model

    nb_timesteps = 2

    def makeXy(ts, nb_timesteps, features='itself'): 
        """ 
        Input:  
            ts: original scaled time series 
            nb_timesteps: number of time steps in the regressors 
            features: itself == use the previous values of the label only
                        all == use previous values of all avaialable data
        Output:  
            X: 2-D array of regressors 
            y: 1-D array of target  
    """
        x_train = []
        y_train = []

        for i in range(nb_timesteps, ts.shape[0]):
            if features == 'itself':
                x_train.append(ts[i-nb_timesteps:i,:-1])
            else:
                x_train.append(ts[i-nb_timesteps:i,0:])
            y_train.append(ts[i, -1])

        x_train, y_train = np.array(x_train), np.array(y_train)
        return x_train, y_train 


    X_train, y_train = makeXy(df_train_scaled, nb_timesteps ,'all')



    # Define LSTM Neural Network
    def create_model():
        regressor = Sequential()

        regressor.add(InputLayer(input_shape=(X_train.shape[1], X_train.shape[2])))
        regressor.add(LSTM(units=500, return_sequences=True, ))
        regressor.add(Dropout(rate=0.3))
        regressor.add(LSTM(units=100, return_sequences=True))
        regressor.add(Dropout(rate=0.2))
        regressor.add(LSTM(units=75, return_sequences=True))
        regressor.add(Dropout(rate = 0.2))
        regressor.add(LSTM(units=50))
        regressor.add(Dropout(rate = 0.2))
        regressor.add(Dense(units=1))
        regressor.compile(loss='mean_squared_error', optimizer = Adam(learning_rate=0.0003))
        return regressor

    # Fit and save best parameters of model
    epoch = 50


    es = EarlyStopping(monitor='val_loss', min_delta=1e-10, patience=10, verbose=1)
    rlr = ReduceLROnPlateau(monitor='val_loss', factor=0.5, patience=10, verbose=1)

    # Model Checkpoint
    model_folder ='dags/adaboost_lstm_model'
    model_file = 'lstm_model.hdf5'
    save_weights_at = os.path.join(model_folder, model_file) 
    mcp = ModelCheckpoint(save_weights_at, monitor='val_loss', verbose=0, 
                                save_best_only=True, save_weights_only=False, mode='min', 
                                save_freq='epoch') 

    tb = TensorBoard('logs')

    ann_estimator = KerasRegressor(build_fn = create_model, shuffle=True, epochs=epoch, callbacks=[es, rlr, mcp, tb], validation_split=0.2, verbose=1, batch_size=32)

    from sklearn.ensemble import AdaBoostRegressor
    boosted_ann = AdaBoostRegressor(base_estimator= ann_estimator,n_estimators=5)
    boosted_ann.fit(X_train, y_train)# scale training data 


    # Train predict
    predicted_price = boosted_ann.predict(X_train)
    predicted_price = scaler_close_price.inverse_transform(predicted_price.reshape(-1,1))
    predicted_price = predicted_price.reshape(X_train.shape[0])



    #save predict result to db 
    train_results = pd.DataFrame(df_train['close'][nb_timesteps:])
    train_results['predict'] = predicted_price
    train_results.reset_index(inplace=True)
    execute_values(conn, train_results, 'model_result')
