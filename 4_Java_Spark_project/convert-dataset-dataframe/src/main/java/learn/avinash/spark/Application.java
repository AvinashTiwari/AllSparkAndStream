package learn.avinash.spark;

public class Application {
    public static void main(String[] args) {
        ArrayToDataset app = new ArrayToDataset();
		app.start();

        CsvToDatasetHouseToDataframe app1 = new CsvToDatasetHouseToDataframe();
        app1.start();

		WordCount wc = new WordCount();
		wc.start();

    }
}
