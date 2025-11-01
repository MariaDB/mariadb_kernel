""" Maintains a list of magic commands supported by the kernel """

# Copyright (c) MariaDB Foundation.
# Distributed under the terms of the Modified BSD License.

from mariadb_kernel.maria_magics.line import Line
from mariadb_kernel.maria_magics.df import DF
from mariadb_kernel.maria_magics.lsmagic import LSMagic
from mariadb_kernel.maria_magics.maria_magic import MariaMagic
from mariadb_kernel.maria_magics.bar import Bar
from mariadb_kernel.maria_magics.pie import Pie
from mariadb_kernel.maria_magics.delimiter import Delimiter
from mariadb_kernel.maria_magics.load import Load
from mariadb_kernel.maria_magics.ml_commands.data_cleaning.missing import Missing
from mariadb_kernel.maria_magics.ml_commands.data_cleaning.dropmissing import DropMissing
from mariadb_kernel.maria_magics.ml_commands.data_cleaning.stats import Stats
from mariadb_kernel.maria_magics.ml_commands.data_cleaning.fillmissing import FillMissing
from mariadb_kernel.maria_magics.ml_commands.data_cleaning.outliers import Outliers
from mariadb_kernel.maria_magics.ml_commands.data_cleaning.dropoutliers import DropOutliers
from mariadb_kernel.maria_magics.ml_commands.data_cleaning.clipoutliers import ClipOutliers
from mariadb_kernel.maria_magics.ml_commands.data_preprocessing.encode import Encode
from mariadb_kernel.maria_magics.ml_commands.data_preprocessing.normalize import Normalize
from mariadb_kernel.maria_magics.ml_commands.data_preprocessing.standardize import Standardize
from mariadb_kernel.maria_magics.ml_commands.data_preprocessing.splitdata import SplitData
from mariadb_kernel.maria_magics.ml_commands.model_training.train_model import TrainModel
from mariadb_kernel.maria_magics.ml_commands.model_training.evaluate_model import EvaluateModel
from mariadb_kernel.maria_magics.ml_commands.model_training.savemodel import SaveModel
from mariadb_kernel.maria_magics.ml_commands.ml_pipeline.select_features import SelectFeatures
from mariadb_kernel.maria_magics.ml_commands.ml_pipeline.select_model import SelectModel
from mariadb_kernel.maria_magics.ml_commands.model_training.loadmodel import LoadModel
from mariadb_kernel.maria_magics.ml_commands.model_training.predict import Predict
from mariadb_kernel.maria_magics.ml_commands.ml_pipeline.ml_pipeline import MLPipeline
from mariadb_kernel.maria_magics.rag_commands.maria_ingest import MariaIngest
from mariadb_kernel.maria_magics.rag_commands.maria_search import MariaSearch
from mariadb_kernel.maria_magics.rag_commands.maria_rag_query import MariaRAGQuery

def get():
    return {
        "line": Line,
        "bar": Bar,
        "pie": Pie,
        "df": DF,
        "lsmagic": LSMagic,
        "delimiter": Delimiter,
        "load": Load,
        "missing": Missing,
        "dropmissing": DropMissing,
        "stats": Stats,
        "fillmissing": FillMissing,
        "outliers": Outliers,
        "dropoutliers": DropOutliers,
        "clipoutliers": ClipOutliers,
        "encode": Encode,
        "normalize": Normalize,
        "standardize": Standardize,
        "splitdata": SplitData,
        "train_model": TrainModel,
        "evaluate_model": EvaluateModel,
        "savemodel": SaveModel,
        "loadmodel": LoadModel,
        "predict": Predict,
        "select_features": SelectFeatures,
        "select_model": SelectModel,
        "ml_pipeline": MLPipeline,
        "maria_ingest": MariaIngest,
        "maria_search": MariaSearch,
        "maria_rag_query": MariaRAGQuery,
    }
