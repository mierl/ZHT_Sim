public class Comparator implements Comparable<Object> {
	long wait;
	int id;

	public Comparator(long wait, int id) {
		this.wait = wait;
		this.id = id;
	}

	public int compareTo(Object o) {
		//System.out.println("compareTo() is called.");
		
		// this method is not really used.
		// 0: =; 1: >; -1: <;
		Comparator comp = (Comparator) o;
		if (comp.wait > wait) {
			return -1;
		} else if (comp.wait < wait) {
			return 1;
		} else if (comp.id > id) {//comp.wait == wait
			return -1;
		} else if (comp.id < id) {
			return 1;
		} else {
			return 0; // everything is same
		}
	}

}
