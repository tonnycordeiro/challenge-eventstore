package net.intelie.challenges;

import static org.junit.Assert.*;

import java.util.*;

import org.junit.Test;

public class EventStoreTest {
	final String TYPE_1 = "type_1";
	final String TYPE_2 = "type_2";
	final long MIN_TIME_STAMP = 0;
	final long MAX_TIME_STAMP = Long.MAX_VALUE;
	
	@Test
    public void insertOneEventInCollection() throws Exception {
		Event expectedEvent = new Event(TYPE_1, 123L);
		Event actualEvent = null;
		
		ConcreteEventStore evtStore = getConcreteEventStore();
		
		evtStore.insert(expectedEvent);
		
		try(ConcreteEventIterator iterator = (ConcreteEventIterator)evtStore.query(TYPE_1, MIN_TIME_STAMP, MAX_TIME_STAMP)){
			if(iterator.moveNext()) {
	        	actualEvent = iterator.current();
	        }	
		}catch (Exception e) {
			throw e;
		}
        
        assertTrue(ConcreteEventStore.getEventCollection().containsKey(TYPE_1));
        assertEquals(expectedEvent, actualEvent);
	}

	private ConcreteEventStore getConcreteEventStore() {
		ConcreteEventStore evtStore = new ConcreteEventStore();
		evtStore.removeAll(TYPE_1);
		evtStore.removeAll(TYPE_2);
		return evtStore;
	}
	
	@Test
    public void getOneEventFromIteratorWithMoreThanOneType() throws Exception {
		Event expectedEvent = new Event(TYPE_1, 123L);
		Event event2 = new Event(TYPE_2, 123L);
		Event event3 = new Event(TYPE_2, 1243L);
		Event actualEvent = null;
		
		ConcreteEventStore evtStore = getConcreteEventStore();
		
		evtStore.insert(expectedEvent);
		evtStore.insert(event2);
		evtStore.insert(event3);
		
		try(ConcreteEventIterator iterator = (ConcreteEventIterator)evtStore.query(TYPE_1, MIN_TIME_STAMP, MAX_TIME_STAMP)){
			if(iterator.moveNext()) {
	        	actualEvent = iterator.current();
	        }	
		}catch (Exception e) {
			throw e;
		}

        assertTrue(ConcreteEventStore.getEventCollection().containsKey(TYPE_1));
        assertEquals(expectedEvent, actualEvent);
	}	

	@Test
    public void insertNullInCollection() throws Exception {
		Event actualEvent = null;
		ConcreteEventStore evtStore = new ConcreteEventStore();
		evtStore.removeAll(TYPE_1);
		evtStore.removeAll(TYPE_2);
		evtStore.insert(actualEvent);
		
        assertEquals(ConcreteEventStore.getEventCollection().size(), 0);
	}
	
	@Test
    public void insertEventsWithOrdering() throws Exception {
		int count = 0;
		boolean isOrdered = true;
		ArrayList<Event> eventList = new ArrayList<>(List.of(
				new Event(TYPE_1, 123L), 
				new Event(TYPE_1, 1234L),
				new Event(TYPE_1, 1234L),
				new Event(TYPE_2, 322L),
				new Event(TYPE_1, 122123L),
				new Event(TYPE_1, 11L),
				new Event(TYPE_1, 100000L)
				)
		);
		
        Event previousEvent = new Event(TYPE_1, -1);
        Event currentEvent = null;
        
        ConcreteEventStore evtStore = getConcreteEventStore();        
        eventList.forEach((evt) -> evtStore.insert(evt));
        
		try(ConcreteEventIterator iterator = (ConcreteEventIterator)evtStore.query(TYPE_1, MIN_TIME_STAMP, MAX_TIME_STAMP)){
			while(iterator.moveNext()) {
	        	count++;
	        	currentEvent = iterator.current();
	        	if(new EventComparator().compare(currentEvent,previousEvent) < 0) {
	        		isOrdered = false;
	        		break;
	        	}
	        }	
		}catch (Exception e) {
			throw e;
		}

        assertEquals(ConcreteEventStore.getEventCollection().size(), 2);
        assertEquals(count, 5);
        assertTrue(isOrdered);
	}	
	
	@Test
    public void insertEventInHeadPosition() throws Exception {
		int count = 0;
		ArrayList<Event> eventList = new ArrayList<>(List.of(
				new Event(TYPE_1, 123L), 
				new Event(TYPE_1, 1234L),
				new Event(TYPE_1, 1234L),
				new Event(TYPE_2, 322L),
				new Event(TYPE_1, 122123L),
				new Event(TYPE_1, 11L),
				new Event(TYPE_1, 100000L)
				)
		);
		
        Event expectedHeadEvent = new Event(TYPE_1, 3L);
        Event actualHeadEvent = null;
        ConcreteEventStore evtStore = getConcreteEventStore();        
        
        eventList.forEach((evt) -> evtStore.insert(evt));
        evtStore.insert(expectedHeadEvent);
        
		try(ConcreteEventIterator iterator = (ConcreteEventIterator)evtStore.query(TYPE_1, MIN_TIME_STAMP, MAX_TIME_STAMP)){
	        while(iterator.moveNext()) {
	        	if(count == 0) {
	        		actualHeadEvent = iterator.current();
	        	}
	        	count++;
	        }
		}catch (Exception e) {
			throw e;
		}
        
        assertEquals(ConcreteEventStore.getEventCollection().size(), 2);
        assertEquals(count, 6);
        assertEquals(expectedHeadEvent, actualHeadEvent);
	}		
	
	@Test
    public void insertEventInLastPosition() throws Exception {
		int count = 0;
		ArrayList<Event> eventList = new ArrayList<>(List.of(
				new Event(TYPE_1, 123L), 
				new Event(TYPE_1, 1234L),
				new Event(TYPE_1, 1234L),
				new Event(TYPE_2, 322L),
				new Event(TYPE_1, 122123L),
				new Event(TYPE_1, 11L),
				new Event(TYPE_1, 100000L)
				)
		);
		
        Event expectedLastEvent = new Event(TYPE_1, 122124L);
        Event actualLastEvent = null;

        ConcreteEventStore evtStore = getConcreteEventStore();
        
        eventList.forEach((evt) -> evtStore.insert(evt));
        evtStore.insert(expectedLastEvent);
        
        try(ConcreteEventIterator iterator = (ConcreteEventIterator)evtStore.query(TYPE_1, MIN_TIME_STAMP, MAX_TIME_STAMP)){
            while(iterator.moveNext()) {
            	count++;
            	if(count == 6) {
            		actualLastEvent = iterator.current();
            	}
            }
        }catch (Exception e) {
			throw e;
		}
        
        assertEquals(ConcreteEventStore.getEventCollection().size(), 2);
        assertEquals(count, 6);
        assertEquals(expectedLastEvent, actualLastEvent);
	}		
	
	@Test
    public void removeOneType() throws Exception {
		int count = 0;
		ArrayList<Event> eventList = new ArrayList<>(List.of(
				new Event(TYPE_1, 123L), 
				new Event(TYPE_1, 1234L),
				new Event(TYPE_1, 1234L),
				new Event(TYPE_2, 322L),
				new Event(TYPE_1, 122123L),
				new Event(TYPE_1, 11L),
				new Event(TYPE_1, 100000L)
				)
		);
		
        ConcreteEventStore evtStore = getConcreteEventStore();        
        eventList.forEach((evt) -> evtStore.insert(evt));
        
        evtStore.removeAll(TYPE_1);
        
        try(ConcreteEventIterator iterator = (ConcreteEventIterator)evtStore.query(TYPE_2, MIN_TIME_STAMP, MAX_TIME_STAMP)){
        	while(iterator.moveNext()) {
            	count++;
            }
        }catch (Exception e) {
			throw e;
		}
        
        try(ConcreteEventIterator emptyIterator = (ConcreteEventIterator)evtStore.query(TYPE_1, MIN_TIME_STAMP, MAX_TIME_STAMP)){
        	assertEquals(emptyIterator.moveNext(), false);
        }catch (Exception e) {
			throw e;
		}

        assertEquals(ConcreteEventStore.getEventCollection().size(), 1);
        assertEquals(count, 1);
	}	
	
	
	@Test
    public void removeAllTypes() throws Exception {
		ArrayList<Event> eventList = new ArrayList<>(List.of(
				new Event(TYPE_1, 123L), 
				new Event(TYPE_1, 1234L),
				new Event(TYPE_1, 1234L),
				new Event(TYPE_2, 322L),
				new Event(TYPE_1, 122123L),
				new Event(TYPE_1, 11L),
				new Event(TYPE_1, 100000L)
				)
		);
		
		ConcreteEventStore evtStore = getConcreteEventStore();        
        eventList.forEach((evt) -> evtStore.insert(evt));
        
        evtStore.removeAll(TYPE_1);
        evtStore.removeAll(TYPE_2);
        
        assertEquals(ConcreteEventStore.getEventCollection().size(), 0);
	}
	
	@Test
    public void ignoreInvalidType() throws Exception {
		ArrayList<Event> eventList = new ArrayList<>(List.of(
				new Event(TYPE_1, 123L), 
				new Event(TYPE_1, 1234L),
				new Event(TYPE_1, 1234L),
				new Event(TYPE_2, 322L),
				new Event(TYPE_1, 122123L),
				new Event(TYPE_1, 11L),
				new Event(TYPE_1, 100000L)
				)
		);
		
		ConcreteEventStore evtStore = getConcreteEventStore();       
        eventList.forEach((evt) -> evtStore.insert(evt));
        
        evtStore.removeAll("INVALID_TYPE");
        
        assertEquals(ConcreteEventStore.getEventCollection().size(), 2);
	}	
    
	@Test
    public void ignoreNullType() throws Exception {
		ArrayList<Event> eventList = new ArrayList<>(List.of(
				new Event(TYPE_1, 123L), 
				new Event(TYPE_1, 1234L),
				new Event(TYPE_1, 1234L),
				new Event(TYPE_2, 322L),
				new Event(TYPE_1, 122123L),
				new Event(TYPE_1, 11L),
				new Event(TYPE_1, 100000L)
				)
		);
		
		ConcreteEventStore evtStore = getConcreteEventStore();        
        eventList.forEach((evt) -> evtStore.insert(evt));
        
        evtStore.removeAll(null);
        
        assertEquals(ConcreteEventStore.getEventCollection().size(), 2);
	}	
	
	@Test
    public void runIteratorInsideBundaries() throws Exception {
		int count = 0;
		ArrayList<Event> eventList = new ArrayList<>(List.of(
				new Event(TYPE_1, 1010L), 
				new Event(TYPE_1, 100L),
				new Event(TYPE_1, 1511L),
				new Event(TYPE_1, 1051L),
				new Event(TYPE_1, 5051L),
				new Event(TYPE_1, 1111L)
				)
		);
		Event minInclusiveBoundaryEvent =  new Event(TYPE_1, 5L);
		Event maxExclusiveBoundaryEvent =  new Event(TYPE_1, 15L);
		Event insideEvent1 = new Event(TYPE_1, 11L);
		Event insideEvent2 = new Event(TYPE_1, 7L);
		
		eventList.add(minInclusiveBoundaryEvent);
		eventList.add(0, maxExclusiveBoundaryEvent);
		eventList.add(3, insideEvent1);
		eventList.add(insideEvent2);
		
		ArrayList<Event> expectedEventsList = new ArrayList<>();
		expectedEventsList.add(minInclusiveBoundaryEvent);
		expectedEventsList.add(insideEvent1);
		expectedEventsList.add(insideEvent2);
		
		ConcreteEventStore evtStore = getConcreteEventStore();       
        eventList.forEach((evt) -> evtStore.insert(evt));
        
        try(ConcreteEventIterator iterator = (ConcreteEventIterator)evtStore.query(TYPE_1, 5, 15)){
            while(iterator.moveNext()) {
            	count++;
            	expectedEventsList.remove(iterator.current());
            }
        }catch (Exception e) {
			throw e;
		}
        
        assertEquals(ConcreteEventStore.getEventCollection().size(), 1);
        assertEquals(count, 3);
        assertEquals(expectedEventsList.size(), 0);
	}	

	@Test
    public void removeHeadByIterator() throws Exception {
		int count = 0;
		
		Event expectedEvent = new Event(TYPE_1, 5);
		Event actualEvent = null;
		
		ArrayList<Event> eventList = new ArrayList<>(List.of(
				new Event(TYPE_1, 123L), 
				new Event(TYPE_1, 1234L),
				new Event(TYPE_1, 1234L),
				new Event(TYPE_1, 322L),
				new Event(TYPE_1, 122123L),
				new Event(TYPE_1, 11L),
				new Event(TYPE_1, 100000L)
				)
		);
		
		eventList.add(expectedEvent);
		ConcreteEventStore evtStore = getConcreteEventStore();       
        eventList.forEach((evt) -> evtStore.insert(evt));
        
        
        try(ConcreteEventIterator iterator = (ConcreteEventIterator)evtStore.query(TYPE_1, MIN_TIME_STAMP, MAX_TIME_STAMP)){
        	if(iterator.moveNext()) {
            	actualEvent = iterator.current(); 
    			iterator.remove();
            }
            for(;iterator.moveNext();count++); 
        }catch (Exception e) {
			throw e;
		}
        
        assertEquals(ConcreteEventStore.getEventCollection().size(), 1);
        assertEquals(count, 6);
        assertEquals(expectedEvent, actualEvent);
	}		
	
	@Test
    public void removeLastEventByIterator() throws Exception {
		int count = 0;
		
		Event expectedEvent = new Event(TYPE_1, 99999999L);
		Event actualEvent = null;
		
		ArrayList<Event> eventList = new ArrayList<>(List.of(
				new Event(TYPE_1, 123L), 
				new Event(TYPE_1, 1234L),
				new Event(TYPE_1, 1234L),
				new Event(TYPE_1, 322L),
				new Event(TYPE_1, 122123L),
				new Event(TYPE_1, 11L),
				new Event(TYPE_1, 100000L)
				)
		);
		
		eventList.add(expectedEvent);
		ConcreteEventStore evtStore = getConcreteEventStore();        
        eventList.forEach((evt) -> evtStore.insert(evt));
        
        
        try(ConcreteEventIterator iterator = (ConcreteEventIterator)evtStore.query(TYPE_1, MIN_TIME_STAMP, MAX_TIME_STAMP)){
            while(iterator.moveNext()) {
            	count++;
            	if(count == 7) {
            		actualEvent = iterator.current();
            		iterator.remove();
            	}
            }
        }catch (Exception e) {
			throw e;
		}
        
        assertEquals(ConcreteEventStore.getEventCollection().size(), 1);
        assertEquals(count, 7);
        assertEquals(expectedEvent, actualEvent);
	}		
	
	@Test
    public void removeAllEventsByIterator() throws Exception {
		int count = 0;
		Event currentEvent;
		
		ArrayList<Event> eventList = new ArrayList<>(List.of(
				new Event(TYPE_1, 123L), 
				new Event(TYPE_1, 1234L),
				new Event(TYPE_1, 322L),
				new Event(TYPE_1, 122123L),
				new Event(TYPE_1, 11L),
				new Event(TYPE_1, 100000L)
				)
		);
		
		ConcreteEventStore evtStore = getConcreteEventStore();        
        eventList.forEach((evt) -> evtStore.insert(evt));
        
        try(ConcreteEventIterator iterator = (ConcreteEventIterator)evtStore.query(TYPE_1, MIN_TIME_STAMP, MAX_TIME_STAMP)){
            while(iterator.moveNext()) {
            	count++;
        		if(eventList.contains(currentEvent = iterator.current())) {
        			eventList.remove(currentEvent);
        		}
        		iterator.remove();
            }
        }catch (Exception e) {
			throw e;
		}

        assertEquals(ConcreteEventStore.getEventCollection().size(), 1);
        assertEquals(eventList.size(), 0);
        assertEquals(count, 6);
	}
	
	@Test
    public void removeRangeEventsByIterator() throws Exception {
		int count = 0;
		ArrayList<Event> eventList = new ArrayList<>(List.of(
				new Event(TYPE_1, 1010L), 
				new Event(TYPE_1, 100L),
				new Event(TYPE_1, 1511L),
				new Event(TYPE_1, 1051L),
				new Event(TYPE_1, 5051L),
				new Event(TYPE_1, 1111L)
				)
		);
		Event minInclusiveBoundaryEvent =  new Event(TYPE_1, 5L);
		Event maxExclusiveBoundaryEvent =  new Event(TYPE_1, 15L);
		Event insideEvent1 = new Event(TYPE_1, 11L);
		Event insideEvent2 = new Event(TYPE_1, 7L);
		
		eventList.add(minInclusiveBoundaryEvent);
		eventList.add(0, maxExclusiveBoundaryEvent);
		eventList.add(3, insideEvent1);
		eventList.add(insideEvent2);
		
		ArrayList<Event> expectedEventsList = new ArrayList<>();
		expectedEventsList.add(minInclusiveBoundaryEvent);
		expectedEventsList.add(insideEvent1);
		expectedEventsList.add(insideEvent2);
		
		ConcreteEventStore evtStore = getConcreteEventStore();        
        eventList.forEach((evt) -> evtStore.insert(evt));
        
        try(ConcreteEventIterator iterator = (ConcreteEventIterator)evtStore.query(TYPE_1, 5, 15)){
            while(iterator.moveNext()) {
            	count++;
            	expectedEventsList.remove(iterator.current());
            	iterator.remove();
            }
        }catch (Exception e) {
			throw e;
		}
        
        try(ConcreteEventIterator iteratorSecondTime = (ConcreteEventIterator)evtStore.query(TYPE_1, 5, 16)){
            count = 0;
            while(iteratorSecondTime.moveNext()) {
            	count++;
            }
        }catch (Exception e) {
			throw e;
		}
        
        assertEquals(ConcreteEventStore.getEventCollection().size(), 1);
        assertEquals(count, 1);
        assertEquals(expectedEventsList.size(), 0);
	}
	
	@Test
    public void failWhenGetCurrentEventByIteratorWithoutMoveNext() throws Exception {
		boolean isIllegalStateException = false;
		ArrayList<Event> eventList = new ArrayList<>(List.of(
				new Event(TYPE_1, 123L), 
				new Event(TYPE_1, 1234L),
				new Event(TYPE_1, 1234L),
				new Event(TYPE_2, 322L),
				new Event(TYPE_1, 122123L),
				new Event(TYPE_1, 11L),
				new Event(TYPE_1, 100000L)
				)
		);
		
        ConcreteEventStore evtStore = getConcreteEventStore();        
        eventList.forEach((evt) -> evtStore.insert(evt));
        
        try(ConcreteEventIterator iterator = (ConcreteEventIterator)evtStore.query(TYPE_1, MIN_TIME_STAMP, MAX_TIME_STAMP)){
        	iterator.current();
        }catch (IllegalStateException e) {
        	isIllegalStateException = true;
		}
        
        assertTrue(isIllegalStateException);
	}
	
	@Test
    public void failWhenGetCurrentEventAfterAllIteration() throws Exception {
		boolean isIllegalStateException = false;
		ArrayList<Event> eventList = new ArrayList<>(List.of(
				new Event(TYPE_1, 123L), 
				new Event(TYPE_1, 1234L),
				new Event(TYPE_2, 322L),
				new Event(TYPE_1, 122123L),
				new Event(TYPE_1, 11L),
				new Event(TYPE_1, 100000L)
				)
		);
		
        ConcreteEventStore evtStore = getConcreteEventStore();        
        eventList.forEach((evt) -> evtStore.insert(evt));
        
        try(ConcreteEventIterator iterator = (ConcreteEventIterator)evtStore.query(TYPE_1, MIN_TIME_STAMP, MAX_TIME_STAMP)) {
        	while(iterator.moveNext());
        	iterator.current();
        }catch (IllegalStateException e) {
        	isIllegalStateException = true;
		}
        
        assertTrue(isIllegalStateException);
	}

	@Test
    public void failWhenRemoveCurrentEventByIteratorWithoutMoveNext() throws Exception {
		boolean isIllegalStateException = false;
		ArrayList<Event> eventList = new ArrayList<>(List.of(
				new Event(TYPE_1, 123L), 
				new Event(TYPE_1, 1234L),
				new Event(TYPE_1, 1234L),
				new Event(TYPE_2, 322L),
				new Event(TYPE_1, 122123L),
				new Event(TYPE_1, 11L),
				new Event(TYPE_1, 100000L)
				)
		);
		
        ConcreteEventStore evtStore = getConcreteEventStore();        
        eventList.forEach((evt) -> evtStore.insert(evt));
        
        try(ConcreteEventIterator iterator = (ConcreteEventIterator)evtStore.query(TYPE_1, MIN_TIME_STAMP, MAX_TIME_STAMP)){
        	iterator.remove();
        }catch (IllegalStateException e) {
        	isIllegalStateException = true;
		}
        
        assertTrue(isIllegalStateException);
	}

	@Test
    public void failWhenRemoveCurrentEventAfterAllIteration() throws Exception {
		boolean isIllegalStateException = false;
		ArrayList<Event> eventList = new ArrayList<>(List.of(
				new Event(TYPE_1, 123L), 
				new Event(TYPE_1, 1234L),
				new Event(TYPE_2, 322L),
				new Event(TYPE_1, 122123L),
				new Event(TYPE_1, 11L),
				new Event(TYPE_1, 100000L)
				)
		);
		
        ConcreteEventStore evtStore = getConcreteEventStore();        
        eventList.forEach((evt) -> evtStore.insert(evt));
        
        try(ConcreteEventIterator iterator = (ConcreteEventIterator)evtStore.query(TYPE_1, MIN_TIME_STAMP, MAX_TIME_STAMP)) {
        	while(iterator.moveNext());
        	iterator.remove();
        }catch (IllegalStateException e) {
        	isIllegalStateException = true;
		}
        
        assertTrue(isIllegalStateException);
	}

	
	

}
