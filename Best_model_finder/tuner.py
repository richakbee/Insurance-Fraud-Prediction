from xgboost import XGBClassifier
from sklearn.model_selection import GridSearchCV
from sklearn.ensemble import RandomForestClassifier
from sklearn.naive_bayes import GaussianNB
from sklearn.svm import SVC

class tuner:
    """
               This class is used while model training . to get the models with best hyper parameters .
               all hyper parameter tuning is done here.

               Written By: richabudhraja8@gmail.com
               Version: 1.0
               Revisions: None
    """

    def __init__(self, file_object, logger_object):
        self.file_obj = file_object
        self.log_writer = logger_object
        pass

    def get_params_for_xgboost(self, x_train, y_train):
        """
                                   Method Name: get_params_for_xgboost
                                   Description: This method defines as xgboost model for classification . It also performs grid search
                                                 to find the best hyper parameters for the classifier.
                                   Output: Returns a xgboost model with best hyper parameters
                                   On Failure: Raise Exception

                                   Written By: richabudhraja8@gmail.com
                                   Version: 1.0
                                   Revisions: None

                                """
        estimator = XGBClassifier(objective='binary:logistic', use_label_encoder=False)
        params_grid = {

                'learning_rate': [0.5, 0.1, 0.01, 0.001],
                'max_depth':  range(8, 10, 1),
                'n_estimators': [10, 50, 100, 200]


            }
        self.log_writer.log(self.file_obj, "Entered get_params_for_xgboost of tuner class!!")
        try:

            grid_cv = GridSearchCV(estimator, param_grid=params_grid, cv=5, return_train_score=False)
            grid_cv.fit(x_train, y_train)

            # fetch the best estimator
            best_estimator = grid_cv.best_estimator_
            best_estimator.fit(x_train, y_train )
            self.log_writer.log(self.file_obj,
                                "get best params for xgboost successful"+ str(grid_cv.best_params_) +".Exited get_params_for_xgboost of tuner class!!")

            return best_estimator

        except Exception as e:
            self.log_writer.log(self.file_obj,
                                'Exception occurred in get_params_for_xgboost of tuner class!! Exception message:' + str(
                                    e))
            self.log_writer.log(self.file_obj,
                                'get best params for xgboost unsuccessful .Exited get_params_for_xgboost of tuner class!!')
            raise e


    def get_params_for_random_forest(self, x_train, y_train):
        """
                                           Method Name: get_params_for_random_forest
                                           Description: This method defines as random_forest model for classification . It also performs grid search
                                                         to find the best hyper parameters for the classifier.
                                           Output: Returns a random_forest model with best hyper parameters
                                           On Failure: Raise Exception

                                           Written By: richabudhraja8@gmail.com
                                           Version: 1.0
                                           Revisions: None

                                        """
        estimator = RandomForestClassifier()

        params_grid = {"n_estimators": [10, 50, 100, 130],
                       "criterion": ['gini', 'entropy'],
                        "max_depth": range(2, 4, 1),
                       "max_features": ['auto', 'log2']
                       }

        self.log_writer.log(self.file_obj, "Entered get_params_for_random_forest of tuner class!!")
        try:

            grid_cv = GridSearchCV(estimator, param_grid=params_grid, cv=5, return_train_score=False)
            grid_cv.fit(x_train, y_train)

            # fetch the best estimator
            best_estimator = grid_cv.best_estimator_
            best_estimator.fit(x_train, y_train)
            self.log_writer.log(self.file_obj,
                                "get best params for random_forest successful ."+ str(grid_cv.best_params_) +".Exited get_params_for_random_forest of tuner class!!")

            return best_estimator

        except Exception as e:
            self.log_writer.log(self.file_obj,
                                'Exception occurred in get_params_for_random_forest of tuner class!! Exception message:' + str(
                                    e))
            self.log_writer.log(self.file_obj,
                                'get best params for random_forest unsuccessful .Exited get_params_for_random_forest of tuner class!!')
            raise e


    def get_params_for_naive_bayes(self, x_train, y_train):
        """
                           Method Name: get_params_for_svm
                           Description: This method defines as naive_bayes model for classification . It also performs grid search
                                         to find the best hyper parameters for the classifier.
                           Output: Returns a naive_bayes model with best hyper parameters
                           On Failure: Raise Exception

                           Written By: richabudhraja8@gmail.com
                           Version: 1.0
                           Revisions: None

                        """
        estimator = GaussianNB()
        params_grid = {
            'var_smoothing': [1e-9,0.1, 0.001, 0.5,0.05,0.01,1e-8,1e-7,1e-6,1e-10,1e-11]

        }
        self.log_writer.log(self.file_obj, "Entered get_params_for_naive_bayes of tuner class!!")
        try:

            grid_cv = GridSearchCV(estimator, param_grid=params_grid, cv=5, return_train_score=False)
            grid_cv.fit(x_train, y_train)

            # fetch the best estimator
            best_estimator = grid_cv.best_estimator_
            best_estimator.fit(x_train, y_train)
            self.log_writer.log(self.file_obj,
                                "get best params for naive_bayes successful ."+str(grid_cv.best_params_) +".Exited get_params_for_naive_bayes of tuner class!!")

            return best_estimator

        except Exception as e:
            self.log_writer.log(self.file_obj,
                                'Exception occurred in get_params_for_naive_bayes of tuner class!! Exception message:' + str(
                                    e))
            self.log_writer.log(self.file_obj,
                                'get best params for naive_bayes unsuccessful .Exited get_params_for_naive_bayes of tuner class!!')
            raise e


    def get_params_for_svm(self, x_train, y_train):
        """
                   Method Name: get_params_for_svm
                   Description: This method defines as SVM model for classification . It also performs grid search
                                 to find the best hyper parameters for the classifier.
                   Output: Returns a SVM model with best hyper parameters
                   On Failure: Raise Exception

                   Written By: richabudhraja8@gmail.com
                   Version: 1.0
                   Revisions: None

                """
        estimator = SVC(gamma='auto')
        params_grid={
           'C': [0.1, 0.5, 1, 10],
            'kernel': ['linear','rbf', 'sigmoid']

        }
        self.log_writer.log(self.file_obj, "Entered get_params_for_svm of tuner class!!")
        try:

            grid_cv = GridSearchCV(estimator, param_grid=params_grid, cv=5 ,return_train_score=False)
            grid_cv.fit(x_train, y_train)

            #fetch the best estimator
            best_estimator=grid_cv.best_estimator_
            best_estimator.fit(x_train, y_train)
            self.log_writer.log(self.file_obj,
                                "get best params for SVM successful ."+ str(grid_cv.best_params_) +".Exited get_params_for_svm of tuner class!!")

            return best_estimator

        except Exception as e:
            self.log_writer.log(self.file_obj,
                                'Exception occurred in get_params_for_svm of tuner class!! Exception message:' + str(e))
            self.log_writer.log(self.file_obj,
                                'get best params for SVM unsuccessful .Exited get_params_for_svm of tuner class!!')
            raise e
