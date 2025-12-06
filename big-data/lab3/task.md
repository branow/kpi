# Laboratory Work №3

**Comparative Analysis of Classification Models**

**Topic:** Application and comparative evaluation of Logistic Regression, SVM, and Random Forest algorithms for a binary classification task.

### Purpose of the work

1. Gain practical skills working with the full ML cycle.
2. Learn to apply **mandatory** data preprocessing, in particular **feature scaling**, and understand which models are affected by it.
3. Learn to train, tune, and evaluate three fundamentally different classification models.
4. Perform a comparative analysis of models using appropriate metrics (Accuracy, Precision, Recall, F1-score).

---

### Dataset

We will use the built-in **Wisconsin Breast Cancer** dataset from `scikit-learn`.

* **Task:** Binary classification (malignant tumor (1) vs benign tumor (0)).
* **Features:** 30 numerical features (radius, texture, perimeter, area, etc.), calculated from a digitized image.

---

### Workflow

### Stage 1: Environment setup and data loading

1. Import the necessary libraries: `numpy`, `pandas`, `matplotlib`, `seaborn`.
2. From `sklearn.datasets` import `load_breast_cancer`.
3. From `sklearn.model_selection` import `train_test_split`.
4. From `sklearn.preprocessing` import `StandardScaler`.
5. Import models: `LogisticRegression`, `SVC`, `RandomForestClassifier`.
6. Import metrics: `accuracy_score`, `precision_score`, `recall_score`, `f1_score`, `confusion_matrix`, `classification_report`.
7. Load the data: `data = load_breast_cancer()`.
8. Create a `pandas.DataFrame` from `data.data` (features) and `data.target` (target variable).

### Stage 2: Exploratory Data Analysis (EDA)

1. Using `pandas` methods, inspect the data:

   * `df.head()` — view the first rows.
   * `df.info()` — verify the absence of missing values and check data types.
   * `df.describe()` — **Critically important!** Look at `mean`, `std`, `min`, `max` for different features. You will immediately notice that `mean radius` (e.g., 14.1) and `mean area` (e.g., 654.8) exist on completely different scales.
2. Check class balance (number of samples for ‘0’ and ‘1’).

### Stage 3: Data preparation for modeling

1. Split the data into the feature matrix (`X`) and target vector (`y`).
2. Split the data into training and test sets with an 80/20 ratio:
   `X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)`
3. **Feature scaling:** This is the most important step of this lab.

   * Create a `StandardScaler` instance: `scaler = StandardScaler()`
   * Fit the scaler **only** on training data: `X_train_scaled = scaler.fit_transform(X_train)`
   * Transform the test data using the same scaler: `X_test_scaled = scaler.transform(X_test)`
   * **Explanation:** We scale the data so that features with large values do not dominate features with small ones. This is critical for Logistic Regression and SVM.

### Stage 4: Training and evaluating the models

For each of the three models, perform the following steps:

1. Create a model instance.
2. Train it.
3. Make predictions on the test set.
4. Calculate and display the metrics.

---

**Model 1: Logistic Regression (Linear approach)**

* **Attention:** Use **scaled** data (`X_train_scaled`, `X_test_scaled`).
* `model_lr = LogisticRegression()`
* `model_lr.fit(X_train_scaled, y_train)`
* `y_pred_lr = model_lr.predict(X_test_scaled)`
* Evaluate `y_pred_lr` vs `y_test`.

**Model 2: Support Vector Machine (Geometric approach)**

* **Attention:** Use **scaled** data.
* Check two kernel options (`kernel`):

  1. `model_svm_linear = SVC(kernel='linear')`
  2. `model_svm_rbf = SVC(kernel='rbf')` (default kernel)
* Train both, make predictions, and evaluate which kernel performs better.

**Model 3: Random Forest (Ensemble approach)**

* **Attention:** Tree-based models are insensitive to feature scaling. For experiment purity, train this model on **original, unscaled** data (`X_train`, `X_test`). This demonstrates that data preprocessing depends on the chosen algorithm.
* `model_rf = RandomForestClassifier(random_state=42)`
* `model_rf.fit(X_train, y_train)`
* `y_pred_rf = model_rf.predict(X_test)`
* Evaluate `y_pred_rf` vs `y_test`.

---

### Stage 5: Analysis and conclusions

Create a summary table with metrics (Accuracy, Precision, Recall, F1) for all tested models (LR, SVM-linear, SVM-rbf, RF).
