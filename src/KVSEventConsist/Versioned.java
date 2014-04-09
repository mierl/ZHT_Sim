import java.util.Comparator;

public class Versioned 
{
	public String value;
	public VectorClock vc;
	
	public Versioned(String value, VectorClock vc)
	{
		this.value = value;
		this.vc = vc;
	}
	
	public static final class HappenedBeforeComparator implements Comparator<Versioned>
	{
		public int compare(Versioned v1, Versioned v2)
		{
			Occurred occurred = v1.vc.compare(v2.vc);
			if (occurred == Occurred.BEFORE)
			{
				return -1;
			}
			else if (occurred == Occurred.AFTER)
			{
				return 1;
			}
			else return 0;
		}
	}
}