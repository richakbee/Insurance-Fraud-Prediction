import numpy
import pandas
import sklearn
from Data_ingestion.data_loader import data_loader
from Data_preprocessing.preprocessing import preprocessing
from Data_preprocessing.clustering import clustering
from Best_model_finder.model_finder import model_finder
from Model_functions.model_functions_fileops import model_functions
from File_operation.file_operation import file_operation
from Application_logging import logger
from datetime import datetime


class train_model:
    def __init__(self):

        # open log writer and file object
        self.file_obj = open("Training_logs/ModelTrainingLog.txt", 'a+')
        self.log_writer = logger.app_logger()
        # send log writer and file object for logging to other classes
        self.data_loader_obj = data_loader(self.log_writer, self.file_obj)
        self.preprocessor = preprocessing(self.log_writer, self.file_obj)
        self.clustering_obj = clustering(self.log_writer, self.file_obj)
        self.model_finder_obj = model_finder(self.log_writer, self.file_obj)
        self.model_functions_obj = model_functions(self.log_writer, self.file_obj)
        self.file_op_obj = file_operation()
        self.training_file_name = 'Training_FileFromDB/InputFile.csv'

    def training_model(self):
        try:
            self.log_writer.log(self.file_obj, "Training started!!")

            self.file_op_obj.createDirectoryForPreprocessing()

            # step1 get the data .
            data = self.data_loader_obj.get_data(self.training_file_name)

            # step 2 set  categorical features as true if there are categorical features in data (optional)
            are_categorical_features = True
            categorical_features = ['policy_csl', 'insured_education_level', 'incident_severity', 'insured_sex',
                                    'property_damage', 'police_report_available',
                                    'incident_type', 'collision_type', 'authorities_contacted','insured_occupation',
                                    'insured_relationship']
            categorical_label = ['fraud_reported']
            categorical_columns = categorical_features + categorical_label

            #saving categorical features list at  'preprocessing_data/categorical_features . csv' to be used at prediction
            location_categorical_flist = 'preprocessing_data/categorical_features.csv'
            self.file_op_obj.save_data_to_file(categorical_features, location_categorical_flist )

            # if (are_categorical_features):
            #     data = self.preprocessor.ensure_categorical_data_type(data, categorical_columns)
            # step2 preprocessing

            # adding a derived variable
            now = datetime.now()
            year = now.date().year

            data['vehicle_age'] = year - data['auto_year']

            # step2.1 remove columns (no columns to remove)
            cols_to_remove = ['policy_number', 'policy_bind_date', 'policy_state', 'insured_zip', 'incident_location',
                              'incident_date', 'incident_state', 'incident_city', 'insured_hobbies', 'auto_make',
                              'auto_model', 'auto_year']

            # saving columns to remove at  'preprocessing_data/columns_to_remove.csv' to be used at prediction to drop same columns
            location_col_drop_list = 'preprocessing_data/columns_to_remove.csv'
            self.file_op_obj.save_data_to_file(cols_to_remove, location_col_drop_list)


            data=self.preprocessor.remove_columns(data, cols_to_remove)

            # replacing '?' in data with NAN values
            data.replace('?', numpy.NAN, inplace=True)

            # step2.2 handle /impute null values if present
            is_null_present, columns_with_null = self.preprocessor.is_null_present(data)

            if is_null_present:
                # check if null is in categorical variables then call categorical imputer .
                #print(data[columns_with_null].dtypes.value_counts()['category'])
                #if (data[columns_with_null].dtypes.value_counts()['category'] > 0):
                data = self.preprocessor.impute_Categorical_values(data, columns_with_null)

                    #  nulls are in non categorical columns then
                    # if categorical columns exist in data then encode them
                if are_categorical_features:
                    data = self.preprocessor.encode_categorical_columns(data, categorical_columns)

                # then call KNN imputer
                data = self.preprocessor.impute_missing_values_KNN(data)

            else:
                if are_categorical_features:
                    data = self.preprocessor.encode_categorical_columns(data, categorical_columns)


            # refer EDA . dropping columns beacuse of high correlation
            cols_with_high_correlation = ['age', 'total_claim_amount']
            #appending column names to file
            location_col_drop_list = 'preprocessing_data/columns_to_remove.csv'
            self.file_op_obj.append_data_to_file(cols_with_high_correlation, location_col_drop_list)

            data = self.preprocessor.remove_columns(data, cols_with_high_correlation)

            # check further which columns do not contribute to predictions
            # if the standard deviation for a column is zero, it means that the column has constant values
            # and they are giving the same output for both the labels (fraud & not fraud)
            # prepare the list of such columns to drop
            col_with_zero_std_deviation = self.preprocessor.get_col_with_zero_std_deviation(data)

            if len(col_with_zero_std_deviation) > 0:
                # appending column names to file
                location_col_drop_list = 'preprocessing_data/columns_to_remove.csv'
                self.file_op_obj.append_data_to_file(col_with_zero_std_deviation, location_col_drop_list)

                data = self.preprocessor.remove_columns(data, col_with_zero_std_deviation)


            # refer EDA . to deal with collinearity , performing VIF with a threshold value.
            VIF_thresh = 8
            final_list_columns, cols_to_drop_VIF = self.preprocessor.calculate_vif_(data, VIF_thresh)

            location_col_drop_list = 'preprocessing_data/columns_to_remove.csv'
            self.file_op_obj.append_data_to_file(cols_to_drop_VIF, location_col_drop_list)

            data = data[final_list_columns]


            # step2.3 separate features & label

            X, Y = self.preprocessor.separate_features_and_label(data, label_column_name='fraud_reported')

            # One hot encoding categorical features in data X
            X_cat = self.preprocessor.one_hot_encode_cagtegorical_col(X, categorical_features)
            # Scaling Numerical Columns in data X
            X_num = self.preprocessor.scale_numerical_columns(X, categorical_features)

            #concat numerical & categorical data together
            X = pandas.concat([X_num, X_cat], axis=1)



            # step3 clustering

            # step3.1 find number of clusters
            no_of_clusters = self.clustering_obj.elbow_plot(X)
            no_of_clusters=2

            if no_of_clusters is not None:
                # step3.2 create the clusters in data
                X = self.clustering_obj.create_clusters(X, no_of_clusters)

                #add the prediction label column back
                X['fraud_reported'] = Y
                # step 4 for each cluster find and save best model

                list_of_clusters = X['Cluster'].unique()

                for i in list_of_clusters:
                    # get data for each
                    cluster_data = X[X['Cluster'] == i]

                    # step 4.1 get cluster features & labels
                    cluster_features = cluster_data.drop(['fraud_reported', 'Cluster'], axis=1)
                    cluster_label = cluster_data['fraud_reported']

                    # step 4.2 split data into test & train
                    x_train, x_test, y_train, y_test = sklearn.model_selection.train_test_split(cluster_features,
                                                                                                cluster_label,
                                                                                                test_size=1 / 3,
                                                                                                random_state=0)

                    #reseting indexes
                    x_train.reset_index(inplace=True, drop=True)
                    x_test.reset_index(inplace=True, drop=True)
                    y_train.reset_index(inplace=True, drop=True)
                    y_test.reset_index(inplace=True, drop=True)



                    # step 4.3 find best model
                    best_model_name, best_model = self.model_finder_obj.get_best_model(x_train, y_train, x_test, y_test)

                    # step 4.4 save the best model
                    save_model_status = self.model_functions_obj.save_model(best_model, best_model_name + str(i))

                # logging the successful Training
                self.log_writer.log(self.file_obj, 'Training Successful!!')
                self.file_obj.close()

            else:
                self.log_writer.log(self.file_obj, 'Training Unsuccessful due to no_of_cluster is none')
                self.file_obj.close()
                raise Exception

        except Exception as e:
            # logging the unsuccessful Training
            self.log_writer.log(self.file_obj, 'Training Unsuccessful!!')
            self.file_obj.close()
            raise e
