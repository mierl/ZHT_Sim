import java.util.ArrayList;

import peersim.core.CommonState;

public class VectorClock 
{
	public ArrayList<ClockEntry> versions;
	public volatile long timestamp;
	
	public VectorClock (ArrayList<ClockEntry> versions, long timestamp)
	{
		this.versions = versions;
		this.timestamp = timestamp;
	}
	
	public void incrementVersion(int nodeId, long time)
	{
		this.timestamp = time;
		boolean found = false;
		int index = 0;
		for (; index < versions.size(); index++)
		{
			if (versions.get(index).nodeId == nodeId)
			{
				found = true;
				break;
			} 
			else if (versions.get(index).nodeId > nodeId)
			{
				found = false;
				break;
			}
		}
		if (found)
		{
			versions.set(index, versions.get(index).incremented());
		}
		else
		{
			versions.add(index, new ClockEntry(nodeId, 1));
		}
	}
	
	public long getMaxVersion()
	{
		long max = -1;
		for (ClockEntry entry: versions)
		{
			max = Math.max(entry.version, max);
		}
		return max;
	}
	
	public VectorClock merge(VectorClock vc)
	{
		VectorClock newVC = new VectorClock(new ArrayList<ClockEntry>(), CommonState.getTime());
		int i = 0, j = 0;
		while (i < versions.size() && j < vc.versions.size())
		{
			ClockEntry v1 = versions.get(i);
			ClockEntry v2 = vc.versions.get(j);
			if (v1.nodeId == v2.nodeId)
			{
				newVC.versions.add(new ClockEntry(v1.nodeId, Math.max(v1.version, v2.version)));
				i++;
				j++;
			}
			else if (v1.nodeId < v2.nodeId)
			{
				newVC.versions.add(new ClockEntry(v1.nodeId, v1.version));
				i++;
			}
			else
			{
				newVC.versions.add(new ClockEntry(v2.nodeId, v2.version));
				j++;
			}
		}
		for (int k = i;k < versions.size();k++)
		{
			newVC.versions.add(new ClockEntry(versions.get(k).nodeId, versions.get(k).version));
		}
		for (int k = j;k < vc.versions.size();k++)
		{
			newVC.versions.add(new ClockEntry(vc.versions.get(k).nodeId, vc.versions.get(k).version));
		}
		return newVC;
	}
	
	public Occurred compare(VectorClock v)
	{
		return compare(this, v);
	}
	
	public static Occurred compare(VectorClock v1, VectorClock v2)
	{
		boolean v1Bigger = false, v2Bigger = false;
		int p1 = 0, p2 = 0;
		while (p1 < v1.versions.size() && p2 < v2.versions.size())
		{
			ClockEntry ver1 = v1.versions.get(p1);
			ClockEntry ver2 = v2.versions.get(p2);
			if (ver1.nodeId == ver2.nodeId)
			{
				if (ver1.version > ver2.version)
				{
					v1Bigger = true;
				}
				else if (ver1.version < ver2.version)
				{
					v2Bigger = true;
				}
				p1++;
				p2++;
			}
			else if (ver1.nodeId > ver2.nodeId)
			{
				v2Bigger = true;
				p2++;
			}
			else
			{
				v1Bigger = true;
				p1++;
			}
		}
		if (p1 < v1.versions.size())
		{
			v1Bigger = true;
		}
		else if (p2 < v2.versions.size())
		{
			v2Bigger = true;
		}
		if (!v1Bigger && !v2Bigger)
		{
			return Occurred.BEFORE;
		}
		else if (v1Bigger && !v2Bigger)
		{
			return Occurred.AFTER;
		}
		else if (!v1Bigger && v2Bigger)
		{
			return Occurred.BEFORE;
		}
		else
		{
			return Occurred.CONCURRENTLY;
		}
	}
}