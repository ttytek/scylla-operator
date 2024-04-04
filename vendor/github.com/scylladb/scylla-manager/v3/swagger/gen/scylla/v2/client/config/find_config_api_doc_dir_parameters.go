// Code generated by go-swagger; DO NOT EDIT.

package config

// This file was generated by the swagger tool.
// Editing this file might prove futile when you re-run the swagger generate command

import (
	"context"
	"net/http"
	"time"

	"github.com/go-openapi/errors"
	"github.com/go-openapi/runtime"
	cr "github.com/go-openapi/runtime/client"
	"github.com/go-openapi/strfmt"
)

// NewFindConfigAPIDocDirParams creates a new FindConfigAPIDocDirParams object
// with the default values initialized.
func NewFindConfigAPIDocDirParams() *FindConfigAPIDocDirParams {

	return &FindConfigAPIDocDirParams{

		timeout: cr.DefaultTimeout,
	}
}

// NewFindConfigAPIDocDirParamsWithTimeout creates a new FindConfigAPIDocDirParams object
// with the default values initialized, and the ability to set a timeout on a request
func NewFindConfigAPIDocDirParamsWithTimeout(timeout time.Duration) *FindConfigAPIDocDirParams {

	return &FindConfigAPIDocDirParams{

		timeout: timeout,
	}
}

// NewFindConfigAPIDocDirParamsWithContext creates a new FindConfigAPIDocDirParams object
// with the default values initialized, and the ability to set a context for a request
func NewFindConfigAPIDocDirParamsWithContext(ctx context.Context) *FindConfigAPIDocDirParams {

	return &FindConfigAPIDocDirParams{

		Context: ctx,
	}
}

// NewFindConfigAPIDocDirParamsWithHTTPClient creates a new FindConfigAPIDocDirParams object
// with the default values initialized, and the ability to set a custom HTTPClient for a request
func NewFindConfigAPIDocDirParamsWithHTTPClient(client *http.Client) *FindConfigAPIDocDirParams {

	return &FindConfigAPIDocDirParams{
		HTTPClient: client,
	}
}

/*
FindConfigAPIDocDirParams contains all the parameters to send to the API endpoint
for the find config api doc dir operation typically these are written to a http.Request
*/
type FindConfigAPIDocDirParams struct {
	timeout    time.Duration
	Context    context.Context
	HTTPClient *http.Client
}

// WithTimeout adds the timeout to the find config api doc dir params
func (o *FindConfigAPIDocDirParams) WithTimeout(timeout time.Duration) *FindConfigAPIDocDirParams {
	o.SetTimeout(timeout)
	return o
}

// SetTimeout adds the timeout to the find config api doc dir params
func (o *FindConfigAPIDocDirParams) SetTimeout(timeout time.Duration) {
	o.timeout = timeout
}

// WithContext adds the context to the find config api doc dir params
func (o *FindConfigAPIDocDirParams) WithContext(ctx context.Context) *FindConfigAPIDocDirParams {
	o.SetContext(ctx)
	return o
}

// SetContext adds the context to the find config api doc dir params
func (o *FindConfigAPIDocDirParams) SetContext(ctx context.Context) {
	o.Context = ctx
}

// WithHTTPClient adds the HTTPClient to the find config api doc dir params
func (o *FindConfigAPIDocDirParams) WithHTTPClient(client *http.Client) *FindConfigAPIDocDirParams {
	o.SetHTTPClient(client)
	return o
}

// SetHTTPClient adds the HTTPClient to the find config api doc dir params
func (o *FindConfigAPIDocDirParams) SetHTTPClient(client *http.Client) {
	o.HTTPClient = client
}

// WriteToRequest writes these params to a swagger request
func (o *FindConfigAPIDocDirParams) WriteToRequest(r runtime.ClientRequest, reg strfmt.Registry) error {

	if err := r.SetTimeout(o.timeout); err != nil {
		return err
	}
	var res []error

	if len(res) > 0 {
		return errors.CompositeValidationError(res...)
	}
	return nil
}
