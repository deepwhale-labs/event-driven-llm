package io.github.deepwhalelabs.eventdrivenllm;

/** Never attach remote bodies, credential-bearing URLs, or transport causes. */
public class CoralException extends RuntimeException {
    public CoralException(String message) {
        super(message);
    }
}
