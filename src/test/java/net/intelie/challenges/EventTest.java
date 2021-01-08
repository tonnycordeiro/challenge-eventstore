package net.intelie.challenges;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class EventTest {
    @Test
    public void thisIsAWarning() throws Exception {
        Event event = new Event("some_type", 123L);

        //THIS IS A WARNING:
        //Some of us (not everyone) are coverage freaks.
        assertEquals(123L, event.timestamp());
        assertEquals("some_type", event.type());
    }
    
    @Test
    public void isEventsComparisonWorking() throws Exception {
        Event event1 = new Event("some_type", 12L);
        Event event2 = new Event("some_type", 123L);
        Event event3 = new Event("some_type", 123L);
        Event event4 = new Event("some_type", 1234L);

        assertTrue(new EventComparator().compare(event1,event2)<0);
        assertTrue(new EventComparator().compare(event2,event3)==0);
        assertTrue(new EventComparator().compare(event4,event3)>0);
    }
    
}