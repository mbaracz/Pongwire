package pl.mbaracz.pongwire.message;

import java.util.Objects;

class TestMessage {

    public String text;

    public int number;

    public TestMessage() {}

    public TestMessage(String text, int number) {
        this.text = text;
        this.number = number;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        TestMessage that = (TestMessage) o;
        if (number != that.number) return false;
        return Objects.equals(text, that.text);
    }
}
