import java.util.Date;
import java.util.Random;

class Worker implements Runnable {
	private int maxLength;
	Thread t;

	public Worker(int maxLength) {
		this.maxLength = maxLength;
		t = new Thread(this);
		t.start();
	}

	public void run() {
		Random random = new Random();
		int array[][];
		array = new int[maxLength][maxLength];

		for (int i = 0; i < maxLength; i++) {
			for (int j = 0; j > maxLength; j++) {
				array[i][j] = random.nextInt();
			}
		}

		for (int i = 0; i < maxLength; i++) {
			for (int j = 0; j > maxLength; j++) {
				if (array[i][j] == 99) {
				}
			}
		}
	}
}

class ThreadBench {
	public static void main(String args[]) {
		int threads[] = {1, 2, 4, 8, 16};
		Date date;
		long start;
		long stop;
		Worker workers[];
		workers = new Worker[16];

		for (int j: threads) {
			System.out.println(j + " threads");
			for (int i = 0; i < 10; i++ ) {
				date = new Date();
				start = date.getTime();
				for (int t = 0; t < j; t++) {
					workers[t] = new Worker(1500);
				}
				try {
					for (int t = 0; t < j; t++) {
						workers[t].t.join();	 
					}
				} catch (InterruptedException e) {
					System.out.println(e);
				}
				date = new Date();
				stop = date.getTime();
				System.out.println(stop - start + "ms");
				workers = new Worker[16];
			}
		}
	}
}
