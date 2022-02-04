public class Measurement {
	
	private int time;
	private double temperature;
	
	// constructor and settters for the measurement
	Measurement(int t, double temp) {
		this.time = t;
		this.temperature = temp;
	}
	
	// getters for time and measurement
	public int getTime() {
		return time;
	}
	
	public double getTemperature() {
		return temperature;
	}
}
