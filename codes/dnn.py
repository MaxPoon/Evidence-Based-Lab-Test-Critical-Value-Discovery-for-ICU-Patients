import tensorflow as tf
import pandas as pd
from sklearn.model_selection import train_test_split
from preprocess import *

def add_layer(inputs, in_size, out_size, activation_function=None):
	Weights = tf.Variable(tf.random_normal([in_size, out_size]))
	biases = tf.Variable(tf.zeros([1, out_size]) + 0.1)
	Wx_plus_b = tf.matmul(inputs, Weights) + biases
	if activation_function is None:
		outputs = Wx_plus_b
	else:
		outputs = activation_function(Wx_plus_b)
	return outputs

def main():
	# load data
	all_time_series_normalized = normalizeTimeSeries(None)
	all_time_series_normalized_concat = pd.concat(all_time_series_normalized)
	all_time_series_normalized_concat_0 = all_time_series_normalized_concat[all_time_series_normalized_concat['NeedInvasive Ventilation']==0]
	all_time_series_normalized_concat_0 = all_time_series_normalized_concat_0.head(272394)
	all_time_series_normalized_concat_1 = all_time_series_normalized_concat[all_time_series_normalized_concat['NeedInvasive Ventilation']==1]
	all_time_series_normalized_concat = pd.concat([all_time_series_normalized_concat_0, all_time_series_normalized_concat_1])
	# split data
	features = ['age', 'gender', 'Respiratory Rate', 'SpO2', 'Temperature', 'Heart Rate', 'CVP', 'Hematocrit', 'Potassium', 'Sodium', 'Creatinine', 'Chloride', 'Urea Nitrogen', 'Platelet Count', 'White Blood Cells', 'Red Blood Cells', 'Calculated Total CO2', 'pH']
	X = all_time_series_normalized_concat[features]
	y = all_time_series_normalized_concat['NeedInvasive Ventilation']
	X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2)
	y_train = np.array([y_train]).T
	y_test = np.array([y_test]).T
	# setup neural network
	print("start setting up nn")
	xs = tf.placeholder(tf.float32, [None, 18])
	ys = tf.placeholder(tf.float32, [None, 1])
	l1 = add_layer(xs, 18, 20, activation_function=tf.nn.relu)
	l2 = add_layer(l1, 20, 20, activation_function=tf.nn.relu)
	prediction = add_layer(l2, 20, 1, activation_function=tf.nn.sigmoid)
	loss = tf.reduce_mean(tf.reduce_sum(tf.square(ys - prediction), reduction_indices=[1]))
	train_step = tf.train.GradientDescentOptimizer(0.7).minimize(loss)
	with tf.Session() as sess:
		if int((tf.__version__).split('.')[1]) < 12 and int((tf.__version__).split('.')[0]) < 1:
			init = tf.initialize_all_variables()
		else:
			init = tf.global_variables_initializer()
		sess.run(init)

		for i in range(1000):
			# compute accuracy
			if i%10 == 0:
				print("Loss after {i} batches: {loss}".format(i=i, loss=sess.run(loss, feed_dict={xs: X_test, ys: y_test})))
				y_pre = sess.run(prediction, feed_dict={xs: X_test})
				y_pre = (y_pre + 0.5).astype(int)
				equality = y_pre == y_test
				print("Accuracy after {i} epochs: {accuracy}".format(i=i, accuracy=np.sum(equality)/len(equality)))
			sess.run(train_step, feed_dict={xs: X_train, ys: y_train})

if __name__ == "__main__":
	main()