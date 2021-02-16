from datetime import datetime

import numpy
import pandas

from Data_ingestion.data_loader import data_loader
from Data_preprocessing_prediction.preprocessing import preprocessing
from Model_functions.model_functions_fileops import model_functions
from Application_logging.logger import app_logger
from File_operation_prediction.file_operation import file_operation
import pandas as pd


class predict_from_model:
    def __init__(self):

        # open log writer and file object
        self.file_obj = open("Prediction_Logs/Prediction_main_log.txt", 'a+')
        self.log_writer = app_logger()
        # send log writer and file object for logging to other classes
        self.data_loader_obj = data_loader(self.log_writer, self.file_obj)
        self.preprocessor = preprocessing(self.log_writer, self.file_obj)
        self.model_functions_obj = model_functions(self.log_writer, self.file_obj)
        self.file_op_obj = file_operation()
        self.prediction_input_file = 'Prediction_FileFromDB/InputFile.csv'
        self.prediction_output_file = 'Prediction_Output_File/Predictions.csv'
        self.categorical_feature_list_loc='preprocessing_data/categorical_features.csv'
        self.columns_to_drop_list_loc = 'preprocessing_data/columns_to_remove.csv'

    def get_prediction_from_model(self):
        try:
            # step 1 delete prediction file from last run
            self.file_op_obj.delete_existing_file_create_new(self.prediction_output_file)

            self.log_writer.log(self.file_obj, "Start of Prediction!!")

            # step 2 get the data from prediction path
            data = self.data_loader_obj.get_data(self.prediction_input_file)

            # step 2 set  categorical features as true if there are categorical features in data (optional)
            are_categorical_features = False
            categorical_features = self.file_op_obj.read_list(self.categorical_feature_list_loc)

            if len(categorical_features)> 0:
                are_categorical_features = True

            if (are_categorical_features):
                data = self.preprocessor.ensure_categorical_data_type(data, categorical_features)
            # step2 preprocessing

            # adding a derived variable
            now = datetime.now()
            year = now.date().year

            data['vehicle_age'] = year - data['auto_year']

            #'policy_no_index' will be used as index
            data['policy_no_index'] = data['policy_number']

            data.set_index(['policy_no_index'], inplace=True)

            # step2.1 remove columns (no columns to remove)
            cols_to_remove = self.file_op_obj.read_list(self.columns_to_drop_list_loc)

            data = self.preprocessor.remove_columns(data, cols_to_remove)

            # replacing '?' in data with NAN values
            data.replace('?', numpy.NAN, inplace=True)

            # step2.2 handle /impute null values if present
            is_null_present, columns_with_null = self.preprocessor.is_null_present(data)

            if is_null_present:
                # check if null is in categorical variables then call categorical imputer .
                # print(data[columns_with_null].dtypes.value_counts()['category'])
                # if (data[columns_with_null].dtypes.value_counts()['category'] > 0):
                data = self.preprocessor.impute_Categorical_values(data, columns_with_null)

                #  nulls are in non categorical columns then
                # if categorical columns exist in data then encode them
                if are_categorical_features:
                    data = self.preprocessor.encode_categorical_columns_from_mapping_file(data, categorical_features)

                # then call KNN imputer
                data = self.preprocessor.impute_missing_values_KNN(data)

            else:
                if are_categorical_features:
                    data = self.preprocessor.encode_categorical_columns_from_mapping_file(data, categorical_features)

            # One hot encoding categorical features in data X
            X_cat = self.preprocessor.one_hot_encode_cagtegorical_col(data, categorical_features)
            # Scaling Numerical Columns in data X
            X_num = self.preprocessor.scale_numerical_columns_from_training_Scaler(data, categorical_features)

            # concat numerical & categorical data together
            data = pandas.concat([X_num, X_cat], axis=1)

            # step 4 load the model & predict from the model
            kmeans = self.model_functions_obj.load_model('Kmeans')

            # step  5 Clustering & prediction
            clusters = kmeans.predict(data)
            data['Cluster'] = clusters

            list_of_clusters = data['Cluster'].unique()
            for i in list_of_clusters:
                # step 5.1 get the data for each
                cluster_data = data[data['Cluster'] == i]
                policy_number= list(cluster_data.index)

                # step 5.3 drop the cluster column
                cluster_data.drop(['Cluster'], axis=1, inplace=True)

                # dont do any  reset index here , with index values we can join with policy number columns to
                #final zip up the values .

                # step 5.4 find correct model name wrt cluster number
                model_name = self.model_functions_obj.find_correct_model_for_cluster(i)
                model = self.model_functions_obj.load_model(model_name)

                # step 5.5 use the model to predict the label
                predictions = []


                results = list(model.predict(cluster_data))  # predict is method of sklearn
                for res in results:
                    if res >= 0 and  res <0.5:
                        predictions.append('N')
                    else:
                        predictions.append('Y')

                # step 5.6 save result in data frame &  append the column saved in step 5.2
                dataframe = pd.DataFrame(list(zip(policy_number, predictions)),
                                         columns=['policy_number', 'Prediction(Fraud/not Fraud)'])

                # step 5.7 save the final result into a csv
                # appends to the csv file
                dataframe.to_csv(self.prediction_output_file, index=False, header=True, mode='a+')

            self.log_writer.log(self.file_obj, "End of Prediction!!")

            return self.prediction_output_file, dataframe.head().to_json(orient="records")

        except Exception as e:
            self.log_writer.log(self.file_obj, "Error Occurred during Prediction!!Error :: %s" % e)
            raise e


