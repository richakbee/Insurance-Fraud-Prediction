import csv
import pickle
from datetime import datetime
import os

import numpy
import pandas
import sklearn
from sklearn.impute import _knn
from joblib import Parallel, delayed
from statsmodels.stats.outliers_influence import variance_inflation_factor


class preprocessing:
    """
            This class shall  be used to clean and transform the data before training.

            Written By: richabudhraja8@gmail.com
            Version: 1.0
            Revisions: None

            """

    def __init__(self, logger_object, file_object):

        self.file_obj = file_object
        self.log_writer = logger_object

    def remove_columns(self, data, column_list):
        """
                Method Name: remove_columns
                Description: TThis method removed the  the columns in the column list from the data.
                Output: Returns A pandas data frame.
                On Failure: Raise Exception

                Written By: richabudhraja8@gmail.com
                Version: 1.0
                Revisions: None

                """
        self.log_writer.log(self.file_obj, "Entered remove_columns of preprocessing class!!")
        try:
            new_data = data.drop(columns=column_list, axis=1)
            self.log_writer.log(self.file_obj,
                                "Column removal Successful.Exited the remove_columns method of the Preprocessor class!!")
            return new_data
        except Exception as e:
            self.log_writer.log(self.file_obj,
                                'Exception occurred in remove_columns method of the Preprocessor class!! Exception message:' + str(
                                    e))
            self.log_writer.log(self.file_obj,
                                'removing columns from data  failed.Error in  remove_columns method of the Preprocessor class!!')
            raise e

    def separate_features_and_label(self, data, label_column_name):
        """
            Method Name: separate_features_and_label
            Description: This method separates the features and a Label Coulmns.
            Output: Returns two separate Dataframes, one containing features and the other containing Labels .
            On Failure: Raise Exception

            Written By: richabudhraja8@gmail.com
            Version: 1.0
            Revisions: None

        """
        self.log_writer.log(self.file_obj, "Entered separate_features_and_label of class preprocessing!!")
        try:
            Y = data[label_column_name]
            X = data.drop(columns=[label_column_name], axis=1)
            self.log_writer.log(self.file_obj,
                                "Label separation successful .Exited separate_features_and_label of class preprocessing!!")
            return X, Y
        except Exception as e:
            self.log_writer.log(self.file_obj,
                                "Label separation unsuccessful !!")
            self.log_writer.log(self.file_obj, "Error in  separate_features_and_label of class preprocessing %s" % e)
            raise e

    def is_null_present(self, X):
        """
                    Method Name: is_null_present
                    Description: This method takes input as dataframe . and tells if there are nulls in any column
                    Output: Returns boolean yes or no .if yes then a csv will be stored will count of null for each column
                            at location "preprocessing_data/null_values.csv". Also returns a list of column names with null values
                    On Failure: Raise Exception

                    Written By: richabudhraja8@gmail.com
                    Version: 1.0
                    Revisions: None

                """
        null_df_loc = "preprocessing_data/"
        self.log_writer.log(self.file_obj,
                            "Entered is_null_present in class preprocessing. Checking for null values in training data")
        bool_value = False
        columns_with_null = []
        now = datetime.now()
        date = now.date()
        time = now.strftime("%H%M%S")
        try:
            count_null = X.isnull().sum()
            for count_ in count_null:
                if count_ > 0:
                    bool_value = True
                    break
            if bool_value:
                null_df = pandas.DataFrame(count_null).reset_index().rename(
                    columns={'index': 'col_name', 0: 'no_of_nulls'})
                file_name = 'null_values_' + str(date) + "_" + str(time) + '.csv'
                dest = os.path.join(null_df_loc, file_name)
                if not os.path.isdir(dest):
                    null_df.to_csv(dest, index=False, header=True)

                # list of columns that has null values
                columns_with_null = list(null_df[null_df['no_of_nulls'] > 0]['col_name'].values)

                self.log_writer.log(self.file_obj,
                                    "Finding missing values is a success.Data written to the null values file at {}. Exited the is_null_present method of the Preprocessor class".format(
                                        null_df_loc))
            return bool_value, columns_with_null
        except Exception as e:
            self.log_writer.log(self.file_obj,
                                'Exception occurred in is_null_present method of the Preprocessor class. Exception message:  ' + str(
                                    e))
            self.log_writer.log(self.file_obj,
                                'Finding missing values failed. Exited the is_null_present method of the Preprocessor class')
            raise e

    def impute_missing_values_KNN(self, X):
        """
                    Method Name: impute_missing_values
                    Description: This method replaces all the missing values in the Dataframe using Categorical imputer then
                                encoding categorical variables and then KNN Imputer for numerical columns
                    Output: Returns A Dataframe which has all the missing values imputed.and
                    On Failure: Raise Exception

                    Written By: richabudhraja8@gmail.com
                    Version: 1.0
                    Revisions: None

                """
        self.log_writer.log(self.file_obj, "Entered the impute_missing_values_KNN method of the Preprocessor class!!")
        data = X
        try:

            imputer = _knn.KNNImputer(n_neighbors=3, weights='uniform', missing_values=numpy.nan)
            # the final result after imputing is an array
            imputed_data = imputer.fit_transform(data)
            # convert the array back to data frame
            new_data = pandas.DataFrame(imputed_data, columns=data.columns)
            self.log_writer.log(self.file_obj,
                                "Imputing missing values Successful. Exited the impute_missing_values_KNN method of the Preprocessor class!!")
            return new_data
        except Exception as e:
            self.log_writer.log(self.file_obj,
                                'Exception occurred in impute_missing_values_KNN method of the Preprocessor class!! Exception message:' + str(
                                    e))
            self.log_writer.log(self.file_obj,
                                'Imputing missing values failed. impute_missing_values_KNN method of the Preprocessor class!!')
            raise e

    def get_col_with_zero_std_deviation(self, X):
        """
        Method Name: get_col_with_zero_std_deviation
        Description: TThis method finds out the columns which have a standard deviation of zero.
        Output: Returns A list of all coulmns which have a standard deviation of zero.
        On Failure: Raise Exception

        Written By: richabudhraja8@gmail.com
        Version: 1.0
        Revisions: None

        """
        self.log_writer.log(self.file_obj, "Entered get_col_with_zero_std_deviation of preprocessing class!!")
        try:
            # get the standard deviation of all columns as data frame , where index is column name .
            std_columns = pandas.DataFrame(X.describe().loc['std'])
            col_list = std_columns[std_columns['std'] == 0].index.to_list()
            self.log_writer.log(self.file_obj,
                                "Column search for Standard Deviation of Zero Successful. Exited the get_columns_with_zero_std_deviation method of the Preprocessor class!!")
            return col_list
        except Exception as e:
            self.log_writer.log(self.file_obj,
                                'Exception occurred in get_col_with_zero_std_deviation method of the Preprocessor class!! Exception message:' + str(
                                    e))
            self.log_writer.log(self.file_obj,
                                'fetching column list with standard deviation of zero failed. get_col_with_zero_std_deviation method of the Preprocessor class!!')
            raise e

    def ensure_categorical_data_type(self, data, categorical_columns):
        """
                Method Name: ensure_categorical_data_type
                Description: TThis method get list of  the columns which have categorical value & the data and
                             changes the data type of the columns as 'category'
                Output: Returns data with categorical columns as categorical type
                On Failure: Raise Exception

                Written By: richabudhraja8@gmail.com
                Version: 1.0
                Revisions: None

                """
        self.log_writer.log(self.file_obj, "Entered ensure_categorical_data_type of preprocessing class!!")
        try:
            cat_dic = {}
            for val in categorical_columns:
                cat_dic[val] = 'category'

            data_new = data.astype(cat_dic)

            self.log_writer.log(self.file_obj,
                                "data type change for categorical features successful. Exited the ensure_categorical_data_type method of the Preprocessor class!!")
            return data_new

        except Exception as e:
            self.log_writer.log(self.file_obj,
                                'Exception occurred in ensure_categorical_data_type method of the Preprocessor class!! Exception message:' + str(
                                    e))
            self.log_writer.log(self.file_obj,
                                'data type change for categorical features  failed. ensure_categorical_data_type method of the Preprocessor class!!')
            raise e

    def encode_categorical_columns(self, data, categorical_columns):
        """
                        Method Name: ensure_categorical_data_type
                        Description: TThis method get list of  the columns which have categorical value & the data and
                                     changes the categorical columns by encoding them
                        Output: Returns data with categorical columns encoded . Save the dictionary used for mapping
                        On Failure: Raise Exception

                        Written By: richabudhraja8@gmail.com
                        Version: 1.0
                        Revisions: None

                        """
        dict_mapping_file_loc = 'preprocessing_data/'
        file_name = 'encoding_mapping_csv_file.csv'
        self.log_writer.log(self.file_obj, "Entered encode_categorical_columns of preprocessing class!!")
        yes_no_col = ['property_damage', 'police_report_available', 'fraud_reported']

        try:
            # create an auto generated mapping dictionary. this will be used lated in prediction too
            dict_col_encode = {}
            temp = {}
            for col in categorical_columns:
                x = data[col].unique()
                temp = dict(list(enumerate(x)))
                dict_col_encode[col] = {v: k for k, v in temp.items()}

            # ensuring yes:1 and no :0 encoded properly
            for key in yes_no_col:
                inner_dict = dict_col_encode[key]
                for inner_key in inner_dict.keys():
                    if inner_key == 'YES' or inner_key == 'Y':
                        inner_dict[inner_key] = 1
                    else:
                        inner_dict[inner_key] = 0

            # saving the dictionary

            dest = os.path.join(dict_mapping_file_loc, file_name)
            if not os.path.isdir(dest):
                with open(dest, 'w') as f:  # You will need 'wb' mode in Python 2.x
                    w = csv.DictWriter(f, dict_col_encode.keys())
                    w.writeheader()
                    w.writerow(dict_col_encode)

            # using the dictionary to encode the data in data frame
            for col in categorical_columns:
                val_dic = dict_col_encode[col]
                data[col] = data[col].apply(lambda x: int(val_dic[
                                                              x]))  # by default it takes as float. will create an issue in Ml algorithms if target columns/label  is float not integer

            self.log_writer.log(self.file_obj,
                                "Encoding successful. Exited encode_categorical_columns of preprocessing class!!")
            return data

        except Exception as e:
            self.log_writer.log(self.file_obj,
                                'Exception occurred in encode_categorical_columns method of the Preprocessor class!! Exception message:' + str(
                                    e))
            self.log_writer.log(self.file_obj,
                                'Encoding categorical features failed. Exited encode_categorical_columns method of the Preprocessor class!!')
            raise e

    def scale_numerical_columns(self, data, categorical_columns):
        """
           Method Name: scale_numerical_columns
           Description: This method get a data frame,& list of categorical columns in the daat & finds the numerical columns and scale them
                        using Minmax scaler() of preprocessing from sklearn.
                        the scaler is saved at location  'preprocessing_data/standardScaler.pkl' for use in prediction.
           Output: Returns data with numerical columns scaled .
           On Failure: Raise Exception

           Written By: richabudhraja8@gmail.com
           Version: 1.0
           Revisions: None

        """
        scaler_path = 'preprocessing_data/MinMaxScaler.pkl'
        self.log_writer.log(self.file_obj, "Entered scale_numerical_columns of preprocessing class!!")
        try:
            # find the numerical columns
            numerical_col_list = list(numpy.setdiff1d(list(data.columns), categorical_columns))
            # get data frames for tarin & test
            df_num= data[numerical_col_list]


            # define standard scaler object
            scaler = sklearn.preprocessing.MinMaxScaler()

            # fitting the scaler on data set
            scaler.fit(df_num)
            # saving the scaler obj
            pickle.dump(scaler, open(scaler_path, 'wb'))

            # transform data set
            df_scaled = scaler.transform(df_num)

            # scaled data is a array convert back to data frame
            Scaled_df= pandas.DataFrame(df_scaled, columns=df_num.columns.tolist(), index=df_num.index)

            self.log_writer.log(self.file_obj,
                                "Scaling numerical columns successful.Exited scale_numerical_columns of preprocessing class!!")
            return Scaled_df

        except Exception as e:
            self.log_writer.log(self.file_obj,
                                'Exception occurred in scale_numerical_columns method of the Preprocessor class!! Exception message:' + str(
                                    e))
            self.log_writer.log(self.file_obj,
                                'Scaling numerical columns failed. Exited scale_numerical_columns method of the Preprocessor class!!')
            raise e

    def impute_Categorical_values(self, data, columns_with_null):
        """
                        Method Name: impute_Categorical_values
                        Description: TThis method get list of  the columns which have categorical value & has nulls
                                     cin the columns. it imputes the null with mode of the column
                        Output: Returns data with imputed value inplace of nulls
                        On Failure: Raise Exception

                        Written By: richabudhraja8@gmail.com
                        Version: 1.0
                        Revisions: None

                        """
        self.log_writer.log(self.file_obj, "Entered impute_Categorical_values of preprocessing class!!")
        try:
            for col in columns_with_null:
                # only for category columns in list of columns that has null values , fill them with mode
                if ((data[col].dtypes) == 'object') or ((data[col].dtypes) == 'category'):
                    data[col].fillna(data[col].mode().values[0], inplace=True)

            self.log_writer.log(self.file_obj,
                                "imputing null value in categorical features successful. Exited the impute_Categorical_values method of the Preprocessor class!!")
            return data

        except Exception as e:
            self.log_writer.log(self.file_obj,
                                'Exception occurred in impute_Categorical_values method of the Preprocessor class!! Exception message:' + str(
                                    e))
            self.log_writer.log(self.file_obj,
                                'imputing null value in categorical features  failed. impute_Categorical_values method of the Preprocessor class!!')
            raise e

    def calculate_vif_(self, data, VIF_thresh):
        """
                                Method Name: calculate_vif_
                                Description: TThis method get the data , calculates VIF  against threshold t
                                Output: Returns list of final columns & a list of columns to drop
                                On Failure: Raise Exception

                                Written By: richabudhraja8@gmail.com
                                Version: 1.0
                                Revisions: None

                                """
        self.log_writer.log(self.file_obj, "Entered calculate_vif_ of preprocessing class!!")
        X = data
        thresh = VIF_thresh

        variables = [X.columns[i] for i in range(X.shape[1])]

        now = datetime.now()
        date = now.date()
        curr_time = now.strftime("%H:%M:%S")
        try:
            dropped = True
            while dropped:
                dropped = False

                vif = Parallel(n_jobs=-1, verbose=5)(delayed(variance_inflation_factor)
                                                     (X[variables].values, ix) for ix in range(len(variables)))

                maxloc = vif.index(max(vif))
                if max(vif) > int(thresh):
                    log_msg = str(curr_time) + ' dropping \'' + X[variables].columns[maxloc] + '\' at index: ' + str(maxloc)

                    self.log_writer.log(self.file_obj, log_msg)

                    variables.pop(maxloc)
                    dropped = True
            columns_to_drop = list(numpy.setdiff1d(list(X.columns), variables))
            self.log_writer.log(self.file_obj,
                                'Calculation of VIF against threshold  Successful.Exiting calculate_vif_  method of the Preprocessor class!!')

            return variables, columns_to_drop

        except Exception as e:
            self.log_writer.log(self.file_obj,
                                'Exception occurred in calculate_vif_  method of the Preprocessor class!! Exception message:' + str(
                                    e))
            self.log_writer.log(self.file_obj,
                                'Calculation of VIF against threshold  failed.Exiting calculate_vif_  method of the Preprocessor class!!')
            raise e

    def one_hot_encode_cagtegorical_col(self, data, categorical_features):

        df_cat = data[categorical_features].copy()
        for col in categorical_features:
            df_cat = pandas.get_dummies(df_cat, columns=[col], prefix=[col], drop_first=True)

        return df_cat
