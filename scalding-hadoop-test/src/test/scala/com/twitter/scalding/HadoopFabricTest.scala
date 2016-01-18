package com.twitter.scalding

import com.twitter.scalding.platform.PlatformTest
import com.twitter.scalding.reducer_estimation.{ RuntimeReducerEstimatorTest, ReducerEstimatorTest, RatioBasedReducerEstimatorTest }

// Keeping all of the specifications in the same tests puts the result output all together at the end.
// This is useful given that the Hadoop MiniMRCluster and MiniDFSCluster spew a ton of logging.
class HadoopPlatformTest
  extends PlatformTest
  with RatioBasedReducerEstimatorTest
  with ReducerEstimatorTest
  with RuntimeReducerEstimatorTest {
  /* just realizing here the tests in a Hadooop (1.x API) context, using cascading-hadoop */
}