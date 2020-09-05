package subpublib;

import java.util.HashSet;
import java.util.Set;

import com.google.common.collect.Sets;

public class TestConcurrentModify {

	
	
	public static void main(String[] args) {

		
		Set<Integer> set1 = Sets.newConcurrentHashSet();
//		HashSet<Integer> set1 = new HashSet<>();
		
		Thread t1 = new Thread(()->
		{
			while(true)
			{
				HashSet<Integer> set = new HashSet<>();
				set.addAll(set1);
				System.out.println("t1");
				try {
					Thread.sleep(10);
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}	
		});
		
		Thread t2 = new Thread(()->
		{
			int i = 0;
			while(true)
			{
				set1.add(i ++);
				System.out.println("t2");
				try {
					Thread.sleep(10);
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
			
		});
		t1.start();
		t2.start();
		
		while(true)
		{
			try {
				Thread.sleep(1000);
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}

	}

}
