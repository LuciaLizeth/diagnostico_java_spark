package minsait.ttaa.datio.common.naming;

public class AgeRange {

	private String rangeName;
	
	private int minAge = -1;
	
	private int maxAge = -1;
	
	
	public AgeRange(String rangeName) {
		this.rangeName = rangeName;
	}

	public String getRangeName() {
		return rangeName;
	}

	public int getMinAge() {
		return minAge;
	}

	public int getMaxAge() {
		return maxAge;
	}
	
	public void setRangeName(String rangeName) {
		this.rangeName = rangeName;
	}

	public void setMinAge(int minAge) {
		this.minAge = minAge;
	}

	public void setMaxAge(int maxAge) {
		this.maxAge = maxAge;
	}
}
