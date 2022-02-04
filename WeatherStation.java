package lsda_assignment_2;
import java.util.*;
import java.util.stream.Collectors;

public class WeatherStation {
	private String city;
	private List<Measurement> measurements = new ArrayList<Measurement>();
	// list of stations being static, common across all objects
	private static List<WeatherStation> stations = new ArrayList<WeatherStation>();
	
	WeatherStation(String city) {
		this.city = city;
		// adding the created object to stations
		stations.add(this);
	}
	
	public void add_Measurement(int time, double temp) {
		measurements.add(new Measurement(time, temp));
	}
	
	// getters for measurements and city of the current instance
	public List<Measurement> getMeasurements() {
		return measurements;
	}
	
	public String getCity() {
		return city;
	}
	
	
	// returning max temperature recorded by the current instance between startTime and endTime
	public Double maxTemperature(int startTime, int endTime) {
		// parallel stream along with the filter conditions and max comparator
		Measurement max_t = getMeasurements().parallelStream().filter(
				m -> m.getTime()>= startTime && m.getTime() <= endTime).max(
				Comparator.comparing(Measurement::getTemperature)).get();
		return max_t.getTemperature();
	}
	
	// mapping the values using t1, t2 as intermediate keys
	public static List mapPairs(Map.Entry m, double t1, double t2, double r) {
		List<Measurement> measures = (List) m.getValue();
		
		// upper and lower bounds for t1 and t2
		double t1_lower = t1 - r; double t1_upper = t1 + r;
		double t2_lower = t2 - r; double t2_upper = t2 + r;
		Vector<Vector> mapped_pairs = new Vector<Vector>();
		
		// checking temperatures of measurements of a weather station
		measures.parallelStream().forEach(
				x -> {
					double temp = x.getTemperature();
					// conditions to count a particular temperature in t1 or(and) t2
					if (temp >= t1_lower && temp <= t1_upper) {
						mapped_pairs.add(new Vector(Arrays.asList(t1, 1.0)));
					}
					if (temp >= t2_lower && temp <= t2_upper) {
						mapped_pairs.add(new Vector(Arrays.asList(t2, 1.0)));
					}
				}
		);
		
		return mapped_pairs;
	}
	
	// generic code for countTemperatures function, which can also work for more than two stations
	public static List countTemperatures(double t1, double t2, double r) {
		// split stations by weather_stations into multiple parts;
		Hashtable<String, List> splitted_data = 
				new Hashtable<String, List>();
		stations.parallelStream().forEach(weather_station -> 
		splitted_data.put(weather_station.getCity(), weather_station.getMeasurements()));
		
		// mapping on each part to see t1, 1 and t2, 1 in each measurement_value;
		List<List> mapped_key_values = splitted_data.entrySet().parallelStream().
				map(x -> mapPairs(x, t1, t2, r)).collect(Collectors.toList());
		
		// shuffle: results from map into two parts - one for t1 instances and one for t2
		Vector<Vector> t1_instances = new Vector<Vector>(); Vector<Vector> t2_instances = new Vector<Vector>();
		
		// bifurcation/shuffling of mapped pairs into t1_instances and t2_instances
		mapped_key_values.parallelStream().forEach(
				x -> {
					x.parallelStream().forEach(
							y -> {
								List<Double> temp_list = (List<Double>)y;
								// checking if a pair has t1 or t2 key, and adding in corresponding vector
								if (temp_list.get(0) == t1) {
									t1_instances.add(new Vector(
											Arrays.asList(temp_list.get(0), temp_list.get(1))));
								}
								if (((List<Double>)y).get(0) == t2) {
									t2_instances.add(new Vector(
											Arrays.asList(temp_list.get(0), temp_list.get(1))));
								}
							}
							
					);
				}
			);
		
		// combining t1 and t2 pairs into shuffled data for further processing, 
		// in order to emulate the parallel reduce operation
		List<List> shuffled_data = new ArrayList<List>();
		shuffled_data.add(t1_instances);
		shuffled_data.add(t2_instances);
		
		// reduce: add all counts of instances according to keys
		// HashTable(synchronized data structure) used as different threads are modifying the same variable having counts
		Hashtable<Double, Double> reduced_data = new Hashtable<Double, Double>();
		reduced_data.put(t1, 0.0); 
		reduced_data.put(t2, 0.0);
		
		shuffled_data.parallelStream().forEach(
				x -> {
					x.stream().forEach(
							y -> {
								List<Double> temp_list = (List<Double>)y;
								// adding all the key-value pairs for each key (i.e. t1 and t2)
								reduced_data.put(
										temp_list.get(0), 
										reduced_data.get(temp_list.get(0)) + temp_list.get(1)); 
							}
					);
				}
		);
		
		// aggegating the final result after calculations to form the result to be returned
		List<List<Double>> result_list = new ArrayList<List<Double>>();
		result_list.add(Arrays.asList(t1, reduced_data.get(t1)));
		result_list.add(Arrays.asList(t2, reduced_data.get(t2)));
		return result_list;
	}
	
	public static void main(String args[]) {
		// creation of two weather stations with city names
		WeatherStation ws_galway = new WeatherStation("Galway");
		WeatherStation ws_dublin = new WeatherStation("Dublin");
		
		// adding measurements to both the instances
		ws_galway.add_Measurement(200, 20.0);
		ws_galway.add_Measurement(400, 11.7);
		ws_galway.add_Measurement(100, -5.4);
		ws_galway.add_Measurement(300, 18.7);
		ws_galway.add_Measurement(450, 20.9);
		ws_dublin.add_Measurement(350, 8.4);
		ws_dublin.add_Measurement(300, 19.2);
		ws_dublin.add_Measurement(450, 7.2);
		System.out.println(ws_galway.maxTemperature(100, 300));
		System.out.println(WeatherStation.countTemperatures(19.0, 10.8, 2.1));
	}
}
