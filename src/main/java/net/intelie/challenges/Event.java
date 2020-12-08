package net.intelie.challenges;

/**
 * This is just an event stub, feel free to expand it if needed.
 */
public class Event implements Comparable<Event>{
    private final String type;
    private final long timestamp;

    public Event(String type, long timestamp) {
        this.type = type;
        this.timestamp = timestamp;
    }

    public String type() {
        return type;
    }

    public long timestamp() {
        return timestamp;
    }
    
    //The Comparable Interface was implemented to allow the easy inserting of events in a list sorted by timestamp
	@Override
	public int compareTo(Event event) {
		return (int) (this.timestamp - event.timestamp);
	}
}
