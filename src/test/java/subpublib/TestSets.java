package subpublib;

import java.util.Set;

import com.google.common.collect.Sets;
import com.google.common.collect.Sets.SetView;

public class TestSets {

	public static void main(String[] args) {
		
		Set<Integer> s1 = Sets.newHashSet();
		Set<Integer> s2 = Sets.newHashSet();
		SetView<Integer> diff = Sets.symmetricDifference(s1, s2);
		System.out.println(diff);
	}

}
