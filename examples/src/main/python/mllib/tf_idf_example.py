#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

from __future__ import print_function

from pyspark import SparkContext
# $example on$
from pyspark.mllib.feature import HashingTF, IDF
# $example off$

if __name__ == "__main__":
    sc = SparkContext(appName="TFIDFExample")  # SparkContext

    # $example on$
    # Load documents (one per line).
    documents = sc.textFile("file:///opt/share/git.repo/spark.git/data/mllib/kmeans_data.txt").map(lambda line: line.split(" "))

    hashingTF = HashingTF()
    tf = hashingTF.transform(documents)

    # While applying HashingTF only needs a single pass to the data, applying IDF needs two passes:
    # First to compute the IDF vector and second to scale the term frequencies by IDF.
    tf.cache()
    idf = IDF().fit(tf)
    tfidf = idf.transform(tf)
    print("tfidf:")
    for each in tfidf.collect():
        print(each)

    # spark.mllib's IDF implementation provides an option for ignoring terms
    # which occur in less than a minimum number of documents.
    # In such cases, the IDF for these terms is set to 0.
    # This feature can be used by passing the minDocFreq value to the IDF constructor.
    """
    idfIgnore = IDF(minDocFreq=0).fit(tf)
    tfidfIgnore = idfIgnore.transform(tf)
    # $example off$
    print("tfidfIgnore:")
    for each in tfidfIgnore.collect():
        print(each)
    """
    sc.stop()

"""

0.0 0.0 0.01
0.1 0.1 0.11
0.2 0.2 0.21
9.0 9.0 9.01
9.1 9.1 9.11
9.2 9.2 9.21


tfidf:
(1048576,[1046921],[3.75828890549])
(1048576,[1046920],[3.75828890549])
(1048576,[1046923],[3.75828890549])
(1048576,[892732],[3.75828890549])
(1048576,[892733],[3.75828890549])
(1048576,[892734],[3.75828890549])
    
tfidfIgnore:
(1048576,[1046921],[0.0])
(1048576,[1046920],[0.0])
(1048576,[1046923],[0.0])
(1048576,[892732],[0.0])
(1048576,[892733],[0.0])
(1048576,[892734],[0.0])
"""