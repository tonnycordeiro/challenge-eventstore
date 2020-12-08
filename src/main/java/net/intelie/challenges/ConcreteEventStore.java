package net.intelie.challenges;

import java.util.concurrent.ConcurrentHashMap;

import net.intelie.challenges.collection.SortedDoublyLinkedList;

/* The data structure ConcurrentHashMap was chosen due to the following reasons:  
 * - It's thread-safety
 * - The both methods "removeAll" and "query" deal with a great amount of data, so it's a good option to access them instantly 
 * 	 by the event type (using a hash map).
 */
public class ConcreteEventStore implements EventStore{
	private volatile static ConcurrentHashMap<String, SortedDoublyLinkedList<Event>> eventCollection;
	
	/* A singleton pattern with a double-checked locking (assuming that there will be a lot of accesses to the object) was implemented because:  
	 * - It's thread-safety
	 * - To store the events in memory and to access them through an unique instance.
	 */
	public static ConcurrentHashMap<String, SortedDoublyLinkedList<Event>> getEventCollection(){
		if(eventCollection == null) {
			synchronized(ConcreteEventStore.class){
				if(eventCollection == null) {
					eventCollection = new ConcurrentHashMap<String, SortedDoublyLinkedList<Event>>();
				}
			}
		}
		
		return eventCollection;
	}
	
	public ConcreteEventStore() {
		eventCollection = getEventCollection();
	}
	
	@Override
	public void insert(Event event) {
		if(event != null) {
			if(!eventCollection.containsKey(event.type())) {
				eventCollection.put(event.type(), new SortedDoublyLinkedList<Event>());
			}
			eventCollection.get(event.type()).insert(event);
		}
	}

	@Override
	public void removeAll(String type) {
		if(type != null)
		{
			if(eventCollection.containsKey(type)) {
				SortedDoublyLinkedList<Event> list = eventCollection.get(type);
				eventCollection.remove(type);
				list.removeAll();
			}
		}
	}

	@Override
	public EventIterator query(String type, long startTime, long endTime) {
		ConcreteEventIterator eventIterator = new ConcreteEventIterator();
		if(eventCollection.containsKey(type)) {
			eventIterator = new ConcreteEventIterator(eventCollection.get(type), startTime, endTime);
		}
		return eventIterator;
	}
	
}
