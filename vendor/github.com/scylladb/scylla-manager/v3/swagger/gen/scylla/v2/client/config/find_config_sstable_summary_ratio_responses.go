// Code generated by go-swagger; DO NOT EDIT.

package config

// This file was generated by the swagger tool.
// Editing this file might prove futile when you re-run the swagger generate command

import (
	"fmt"
	"io"
	"strings"

	"github.com/go-openapi/runtime"
	"github.com/go-openapi/strfmt"

	"github.com/scylladb/scylla-manager/v3/swagger/gen/scylla/v2/models"
)

// FindConfigSstableSummaryRatioReader is a Reader for the FindConfigSstableSummaryRatio structure.
type FindConfigSstableSummaryRatioReader struct {
	formats strfmt.Registry
}

// ReadResponse reads a server response into the received o.
func (o *FindConfigSstableSummaryRatioReader) ReadResponse(response runtime.ClientResponse, consumer runtime.Consumer) (interface{}, error) {
	switch response.Code() {
	case 200:
		result := NewFindConfigSstableSummaryRatioOK()
		if err := result.readResponse(response, consumer, o.formats); err != nil {
			return nil, err
		}
		return result, nil
	default:
		result := NewFindConfigSstableSummaryRatioDefault(response.Code())
		if err := result.readResponse(response, consumer, o.formats); err != nil {
			return nil, err
		}
		if response.Code()/100 == 2 {
			return result, nil
		}
		return nil, result
	}
}

// NewFindConfigSstableSummaryRatioOK creates a FindConfigSstableSummaryRatioOK with default headers values
func NewFindConfigSstableSummaryRatioOK() *FindConfigSstableSummaryRatioOK {
	return &FindConfigSstableSummaryRatioOK{}
}

/*
FindConfigSstableSummaryRatioOK handles this case with default header values.

Config value
*/
type FindConfigSstableSummaryRatioOK struct {
	Payload float64
}

func (o *FindConfigSstableSummaryRatioOK) GetPayload() float64 {
	return o.Payload
}

func (o *FindConfigSstableSummaryRatioOK) readResponse(response runtime.ClientResponse, consumer runtime.Consumer, formats strfmt.Registry) error {

	// response payload
	if err := consumer.Consume(response.Body(), &o.Payload); err != nil && err != io.EOF {
		return err
	}

	return nil
}

// NewFindConfigSstableSummaryRatioDefault creates a FindConfigSstableSummaryRatioDefault with default headers values
func NewFindConfigSstableSummaryRatioDefault(code int) *FindConfigSstableSummaryRatioDefault {
	return &FindConfigSstableSummaryRatioDefault{
		_statusCode: code,
	}
}

/*
FindConfigSstableSummaryRatioDefault handles this case with default header values.

unexpected error
*/
type FindConfigSstableSummaryRatioDefault struct {
	_statusCode int

	Payload *models.ErrorModel
}

// Code gets the status code for the find config sstable summary ratio default response
func (o *FindConfigSstableSummaryRatioDefault) Code() int {
	return o._statusCode
}

func (o *FindConfigSstableSummaryRatioDefault) GetPayload() *models.ErrorModel {
	return o.Payload
}

func (o *FindConfigSstableSummaryRatioDefault) readResponse(response runtime.ClientResponse, consumer runtime.Consumer, formats strfmt.Registry) error {

	o.Payload = new(models.ErrorModel)

	// response payload
	if err := consumer.Consume(response.Body(), o.Payload); err != nil && err != io.EOF {
		return err
	}

	return nil
}

func (o *FindConfigSstableSummaryRatioDefault) Error() string {
	return fmt.Sprintf("agent [HTTP %d] %s", o._statusCode, strings.TrimRight(o.Payload.Message, "."))
}
