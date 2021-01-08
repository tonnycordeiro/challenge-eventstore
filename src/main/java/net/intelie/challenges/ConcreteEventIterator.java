package net.intelie.challenges;

import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.concurrent.ConcurrentSkipListSet;


/* The data structure ConcurrentSkipListSet was chosen due to the following reasons:
 *  - It's thread-safety
 *  - A set data structure is interesting to ignore duplicate events 
 *  - It can be sorted by timestamp and the iterator was set up to iterate according to a range of timestamps 
 */

public class ConcreteEventIterator implements EventIterator { // LinkedEventIterator
	private ConcurrentSkipListSet<Event> sortedList;
	private Iterator<Event> iterator;

	private Event currentEvent;
	private long endTime;
	private boolean wasMovedNext;

	public ConcreteEventIterator() {
		this.currentEvent = null;
		this.wasMovedNext = false;
		sortedList = new ConcurrentSkipListSet<Event>();
		this.iterator = sortedList.iterator();
	}

	/***
	 * 
	 * @param list:      Concurrent Skip List Set of events sorted by a comparator
	 * @param startTime: Start timestamp (inclusive)
	 * @param endTime:   End timestamp (exclusive)
	 */
	public ConcreteEventIterator(ConcurrentSkipListSet<Event> sortedEventListSet, long startTime, long endTime) {
		boolean isNecessaryStartIterating = false;
		this.sortedList = sortedEventListSet;
		this.endTime = endTime;
		this.wasMovedNext = false;
		this.currentEvent = null;
		
		Event lastEventOutOfRange = sortedEventListSet.stream()
								  .filter(evt -> evt.timestamp() < startTime)
								  .sorted(sortedEventListSet.comparator())
								  .reduce((first, second) -> second)
								  .orElse(null);
		
		this.iterator = sortedEventListSet.iterator();
		
		isNecessaryStartIterating = (lastEventOutOfRange != null);
		if(isNecessaryStartIterating) 
		{
			while(iterator.hasNext()) {
				if (iterator.next().equals(lastEventOutOfRange)) {
					break;
				}
			}
		}
	}

	@Override
	public void close() throws Exception {
		this.sortedList = null;
		this.iterator = null;
		this.currentEvent = null;
	}

	@Override
	public boolean moveNext() {
		if (this.sortedList == null)
			return false;

		try {
			this.currentEvent = this.iterator.next();
			this.wasMovedNext = (this.currentEvent.timestamp() < endTime);
		} catch (NoSuchElementException exception) {
			this.currentEvent = null;
			this.wasMovedNext = false;
		}
		return this.wasMovedNext;
	}

	@Override
	public Event current() {
		if (!this.wasMovedNext)
			throw new IllegalStateException();

		return (Event) this.currentEvent;
	}

	@Override
	public void remove() {
		if (!this.wasMovedNext)
			throw new IllegalStateException();
		
		this.iterator.remove();
	}
}
