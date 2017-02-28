package hex.pca;

import hex.DataInfo;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import water.DKV;
import water.Key;
import water.TestUtil;
import water.fvec.Frame;
import water.Scope;

import java.util.concurrent.ExecutionException;

/**
 * Created by wendycwong on 2/27/17.
 */
public class PCAWideDataSetsTests_GramSVD extends TestUtil {
  public static final double TOLERANCE = 1e-6;
  @BeforeClass
  public static void setup() { stall_till_cloudsize(1); }

  /*
  This unit test uses the prostate datasets with NAs, calculate the eigenvectors/values with original PCA implementation.
  Next, it calculates the eigenvectors/values using PCA with wide dataset flag set to true.  Then, we
  compare the eigenvalues/vectors from both methods and they should agree.  Dataset contains numerical and
  categorical columns.
  */
  @Test
  public void testWideDataSetsWithNAs_GramSVD() throws InterruptedException, ExecutionException {
    Scope.enter();
    PCAModel modelN = null;     // store PCA models generated with original implementation
    PCAModel modelW = null;     // store PCA models generated with wideDataSet set to true
    Frame train = null, scoreN = null, scoreW = null;
    try {
      train = parse_test_file(Key.make("prostate_catNA.hex"), "smalldata/prostate/prostate_cat.csv");
      Scope.track(train);
      train.vec(0).setNA(0);
      train.vec(3).setNA(10);
      train.vec(5).setNA(100);
      DKV.put(train);
      PCAModel.PCAParameters parms = new PCAModel.PCAParameters();
      parms._train = train._key;
      parms._k = 7;
      parms._transform = DataInfo.TransformType.STANDARDIZE;
      parms._use_all_factor_levels = true;
      parms._pca_method = PCAModel.PCAParameters.Method.GramSVD;
      parms._impute_missing=false;
      parms._seed = 12345;

      PCA pcaParms = new PCA(parms);
      modelN = pcaParms.trainModel().get(); // get normal data
      scoreN = modelN.score(train);
      Scope.track(scoreN);

      PCA pcaParmsW = new PCA(parms);
      pcaParmsW.setWideDataset(true);  // force to treat dataset as wide even though it is not.
      modelW = pcaParmsW.trainModel().get();
      scoreW = modelW.score(train);
      Scope.track(scoreW);

      // check to make sure eigenvalues and eigenvectors are the same
      // compare eigenvectors and eigenvalues generated by original PCA and wide dataset PCA.
      TestUtil.checkStddev(modelW._output._std_deviation, modelN._output._std_deviation, TOLERANCE);
      boolean[] flippedEig = TestUtil.checkEigvec( modelW._output._eigenvectors, modelN._output._eigenvectors, TOLERANCE);
      TestUtil.checkProjection(scoreW, scoreN, TOLERANCE, flippedEig);
      // Build a POJO, check results with original PCA
      Assert.assertTrue(modelN.testJavaScoring(train,scoreN,TOLERANCE));
      // Build a POJO, check results with wide dataset PCA
      Assert.assertTrue(modelW.testJavaScoring(train,scoreW,TOLERANCE));
    } finally {
      Scope.exit();
      if (modelN != null) modelN.delete();
      if (modelW != null) modelW.delete();
    }
  }

  /*
  This unit test uses the prostate datasets, calculate the eigenvectors/values with original PCA implementation.
  Next, it calculates the eigenvectors/values using PCA with wide dataset flag set to true.  Then, we
  compare the eigenvalues/vectors from both methods and they should agree.  Dataset contains numerical and
  categorical columns.
  */
  @Test public void testWideDataSets_GramSVD() throws InterruptedException, ExecutionException {
    Scope.enter();
    PCAModel modelN = null;     // store PCA models generated with original implementation
    PCAModel modelW = null;     // store PCA models generated with wideDataSet set to true
    Frame train = null, scoreN = null, scoreW = null;
    try {
      train = parse_test_file(Key.make("prostate_cat.hex"), "smalldata/prostate/prostate_cat.csv");
      Scope.track(train);
      PCAModel.PCAParameters parms = new PCAModel.PCAParameters();
      parms._train = train._key;
      parms._k = 7;
      parms._transform = DataInfo.TransformType.STANDARDIZE;
      parms._use_all_factor_levels = true;
      parms._pca_method = PCAModel.PCAParameters.Method.GramSVD;
      parms._impute_missing=false;
      parms._seed = 12345;

      PCA pcaParms = new PCA(parms);
      modelN = pcaParms.trainModel().get(); // get normal data
      scoreN = modelN.score(train);
      Scope.track(scoreN);

      PCA pcaParmsW = new PCA(parms);
      pcaParmsW.setWideDataset(true);  // force to treat dataset as wide even though it is not.
      modelW = pcaParmsW.trainModel().get();
      scoreW = modelW.score(train);
      Scope.track(scoreW);

      // check to make sure eigenvalues and eigenvectors are the same
      // compare eigenvectors and eigenvalues generated by original PCA and wide dataset PCA.
      TestUtil.checkStddev(modelW._output._std_deviation, modelN._output._std_deviation, TOLERANCE);
      boolean[] flippedEig = TestUtil.checkEigvec( modelW._output._eigenvectors, modelN._output._eigenvectors, TOLERANCE);
      TestUtil.checkProjection(scoreN, scoreW, TOLERANCE, flippedEig);
      // Build a POJO, check results with original PCA
      Assert.assertTrue(modelN.testJavaScoring(train,scoreN,TOLERANCE));
      // Build a POJO, check results with wide dataset PCA
      Assert.assertTrue(modelW.testJavaScoring(train,scoreW,TOLERANCE));
    } finally {
      Scope.exit();
      if (modelN != null) modelN.delete();
      if (modelW != null) modelW.delete();
    }
  }

  /*
  This unit test uses a small datasets, calculate the eigenvectors/values with original PCA implementation.
  Next, it calculates the eigenvectors/values using PCA with wide dataset flag set to true.  Then, we
  compare the eigenvalues/vectors from both methods and they should agree.  In this case, we only
  have numerical columns and no categorical columns.
  */
  @Test public void testWideDataSetsSmallDataNumeric_GramSVD() throws InterruptedException, ExecutionException {
    Scope.enter();
    PCAModel modelN = null;     // store PCA models generated with original implementation
    PCAModel modelW = null;     // store PCA models generated with wideDataSet set to true
    Frame train = null, scoreN = null, scoreW = null;
    try {
      train = parse_test_file(Key.make("decathlonN.hex"), "smalldata/pca_test/decathlon.csv");
      Scope.track(train);
      train.remove(12).remove();    // remove categorical columns
      train.remove(11).remove();
      train.remove(10).remove();
      DKV.put(train);
      PCAModel.PCAParameters parms = new PCAModel.PCAParameters();
      parms._train = train._key;
      parms._k = 3;
      parms._transform = DataInfo.TransformType.NONE;
      parms._use_all_factor_levels = true;
      parms._pca_method = PCAModel.PCAParameters.Method.GramSVD;
      parms._impute_missing=false;
      parms._seed = 12345;

      PCA pcaParms = new PCA(parms);
      modelN = pcaParms.trainModel().get(); // get normal data
      scoreN = modelN.score(train);
      Scope.track(scoreN);

      PCA pcaParmsW = new PCA(parms);
      pcaParmsW.setWideDataset(true);  // force to treat dataset as wide even though it is not.
      modelW = pcaParmsW.trainModel().get();
      scoreW = modelW.score(train);
      Scope.track(scoreW);

      // compare eigenvectors and eigenvalues generated by original PCA and wide dataset PCA.
      TestUtil.checkStddev(modelW._output._std_deviation, modelN._output._std_deviation, TOLERANCE);
      boolean[] flippedEig = TestUtil.checkEigvec( modelW._output._eigenvectors, modelN._output._eigenvectors, TOLERANCE);
      TestUtil.checkProjection(scoreW, scoreN, TOLERANCE, flippedEig);
      // Build a POJO, check results with original PCA
      Assert.assertTrue(modelN.testJavaScoring(train,scoreN,TOLERANCE));
      // Build a POJO, check results with wide dataset PCA
      Assert.assertTrue(modelW.testJavaScoring(train,scoreW,TOLERANCE));
    } finally {
      Scope.exit();
      if (modelN != null) modelN.delete();
      if (modelW != null) modelW.delete();
    }
  }



  /*
  This unit test uses a small datasets, calculate the eigenvectors/values with original PCA implementation.
  Next, it calculates the eigenvectors/values using PCA with wide dataset flag set to true.  Then, we
  compare the eigenvalues/vectors from both methods and they should agree.  In this case, we only
  have numerical columns and no categorical columns.
  */
  @Test public void testWideDataSetsSmallDataNumericNAs_GramSVD() throws InterruptedException, ExecutionException {
    Scope.enter();
    PCAModel modelN = null;     // store PCA models generated with original implementation
    PCAModel modelW = null;     // store PCA models generated with wideDataSet set to true
    Frame train = null, scoreN = null, scoreW = null;
    try {
      train = parse_test_file(Key.make("decathlonNNA.hex"), "smalldata/pca_test/decathlon.csv");
      Scope.track(train);
      train.remove(12).remove();    // remove categorical columns
      train.remove(11).remove();
      train.remove(10).remove();

      // set NAs
      train.vec(0).setNA(0);
      train.vec(3).setNA(10);
      train.vec(5).setNA(20);
      DKV.put(train);

      PCAModel.PCAParameters parms = new PCAModel.PCAParameters();
      parms._train = train._key;
      parms._k = 3;
      parms._transform = DataInfo.TransformType.NONE;
      parms._use_all_factor_levels = true;
      parms._pca_method = PCAModel.PCAParameters.Method.GramSVD;
      parms._impute_missing=false;
      parms._seed = 12345;

      PCA pcaParms = new PCA(parms);
      modelN = pcaParms.trainModel().get(); // get normal data
      scoreN = modelN.score(train);
      Scope.track(scoreN);

      PCA pcaParmsW = new PCA(parms);
      pcaParmsW.setWideDataset(true);  // force to treat dataset as wide even though it is not.
      modelW = pcaParmsW.trainModel().get();
      scoreW = modelW.score(train);
      Scope.track(scoreW);

      // compare eigenvectors and eigenvalues generated by original PCA and wide dataset PCA.
      TestUtil.checkStddev(modelW._output._std_deviation, modelN._output._std_deviation, TOLERANCE);
      boolean[] flippedEig = TestUtil.checkEigvec( modelW._output._eigenvectors, modelN._output._eigenvectors, TOLERANCE);
      TestUtil.checkProjection(scoreW, scoreN, TOLERANCE, flippedEig);
      // Build a POJO, check results with original PCA
      Assert.assertTrue(modelN.testJavaScoring(train,scoreN,TOLERANCE));
      // Build a POJO, check results with wide dataset PCA
      Assert.assertTrue(modelW.testJavaScoring(train,scoreW,TOLERANCE));
    } finally {
      Scope.exit();
      if (modelN != null) modelN.delete();
      if (modelW != null) modelW.delete();
    }
  }


  /*
  This unit test uses a small datasets, calculate the eigenvectors/values with original PCA implementation.
  Next, it calculates the eigenvectors/values using PCA with wide dataset flag set to true.  Then, we
  compare the eigenvalues/vectors from both methods and they should agree.  Dataset contains numerical and
  categorical columns.
  */
  @Test public void testWideDataSetsSmallData_GramSVD() throws InterruptedException, ExecutionException {
    Scope.enter();
    PCAModel modelN = null;     // store PCA models generated with original implementation
    PCAModel modelW = null;     // store PCA models generated with wideDataSet set to true
    Frame train = null, scoreN = null, scoreW = null;
    try {
      train = parse_test_file(Key.make("decathlon.hex"), "smalldata/pca_test/decathlon.csv");
      Scope.track(train);
      PCAModel.PCAParameters parms = new PCAModel.PCAParameters();
      parms._train = train._key;
      parms._k = 3;
      parms._transform = DataInfo.TransformType.NONE;
      parms._use_all_factor_levels = true;
      parms._pca_method = PCAModel.PCAParameters.Method.GramSVD;
      parms._impute_missing=false;
      parms._seed = 12345;

      PCA pcaParms = new PCA(parms);
      modelN = pcaParms.trainModel().get(); // get normal data
      scoreN = modelN.score(train);
      Scope.track(scoreN);

      PCA pcaParmsW = new PCA(parms);
      pcaParmsW.setWideDataset(true);  // force to treat dataset as wide even though it is not.
      modelW = pcaParmsW.trainModel().get();
      scoreW = modelW.score(train);
      Scope.track(scoreW);

      // compare eigenvectors and eigenvalues generated by original PCA and wide dataset PCA.
      TestUtil.checkStddev(modelW._output._std_deviation, modelN._output._std_deviation, TOLERANCE);
      boolean[] flippedEig = TestUtil.checkEigvec( modelW._output._eigenvectors, modelN._output._eigenvectors, TOLERANCE);
      TestUtil.checkProjection(scoreW, scoreN, TOLERANCE, flippedEig);
      // Build a POJO, check results with original PCA
      Assert.assertTrue(modelN.testJavaScoring(train,scoreN,TOLERANCE));
      // Build a POJO, check results with wide dataset PCA
      Assert.assertTrue(modelW.testJavaScoring(train,scoreW,TOLERANCE));
    } finally {
      Scope.exit();
      if (modelN != null) modelN.delete();
      if (modelW != null) modelW.delete();
    }
  }


  /*
  This unit test uses a small datasets, calculate the eigenvectors/values with original PCA implementation.
  Next, it calculates the eigenvectors/values using PCA with wide dataset flag set to true.  Then, we
  compare the eigenvalues/vectors from both methods and they should agree.  Dataset contains numerical and
  categorical columns.
  */
  @Test public void testWideDataSetsSmallDataNA_GramSVD() throws InterruptedException, ExecutionException {
    Scope.enter();
    PCAModel modelN = null;     // store PCA models generated with original implementation
    PCAModel modelW = null;     // store PCA models generated with wideDataSet set to true
    Frame train = null, scoreN = null, scoreW = null;
    try {
      train = parse_test_file(Key.make("decalthonNA.hex"), "smalldata/pca_test/decathlon.csv");
      Scope.track(train);
      // set NAs
      train.vec(0).setNA(0);
      train.vec(3).setNA(10);
      train.vec(5).setNA(20);
      DKV.put(train);

      PCAModel.PCAParameters parms = new PCAModel.PCAParameters();
      parms._train = train._key;
      parms._k = 3;
      parms._transform = DataInfo.TransformType.NONE;
      parms._use_all_factor_levels = true;
      parms._pca_method = PCAModel.PCAParameters.Method.GramSVD;
      parms._impute_missing=false;
      parms._seed = 12345;

      PCA pcaParms = new PCA(parms);
      modelN = pcaParms.trainModel().get(); // get normal data
      scoreN = modelN.score(train);
      Scope.track(scoreN);

      PCA pcaParmsW = new PCA(parms);
      pcaParmsW.setWideDataset(true);  // force to treat dataset as wide even though it is not.
      modelW = pcaParmsW.trainModel().get();
      scoreW = modelW.score(train);
      Scope.track(scoreW);

      // compare eigenvectors and eigenvalues generated by original PCA and wide dataset PCA.
      TestUtil.checkStddev(modelW._output._std_deviation, modelN._output._std_deviation, TOLERANCE);
      boolean[] flippedEig = TestUtil.checkEigvec( modelW._output._eigenvectors, modelN._output._eigenvectors, TOLERANCE);
      TestUtil.checkProjection(scoreW, scoreN, TOLERANCE, flippedEig);
      // Build a POJO, check results with original PCA
      Assert.assertTrue(modelN.testJavaScoring(train,scoreN,TOLERANCE));
      // Build a POJO, check results with wide dataset PCA
      Assert.assertTrue(modelW.testJavaScoring(train,scoreW,TOLERANCE));
    } finally {
      Scope.exit();
      if (modelN != null) modelN.delete();
      if (modelW != null) modelW.delete();
    }
  }
}
