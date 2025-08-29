package com.hdfcbank.neftil.camt5254.processor.exception;


import com.hdfcbank.neftil.camt5254.processor.model.Fault;

import java.util.List;

public class Camt5254ProcessorException extends RuntimeException {
    private static final long serialVersionUID = 1L;
    List<Fault> errors;

    public Camt5254ProcessorException(String message) {
        super(message);
    }

    public Camt5254ProcessorException() {

    }

    public Camt5254ProcessorException(String message, Throwable e) {
        super(message, e);
    }

    public Camt5254ProcessorException(String message, List<Fault> errors) {
        super(message);
        this.errors = errors;
    }

    public List<Fault> getErrors() {
        return errors;
    }

    public void setErrors(List<Fault> errors) {
        this.errors = errors;
    }
}
