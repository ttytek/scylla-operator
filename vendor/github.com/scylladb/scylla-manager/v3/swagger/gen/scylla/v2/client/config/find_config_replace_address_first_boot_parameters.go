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

// NewFindConfigReplaceAddressFirstBootParams creates a new FindConfigReplaceAddressFirstBootParams object
// with the default values initialized.
func NewFindConfigReplaceAddressFirstBootParams() *FindConfigReplaceAddressFirstBootParams {

	return &FindConfigReplaceAddressFirstBootParams{

		timeout: cr.DefaultTimeout,
	}
}

// NewFindConfigReplaceAddressFirstBootParamsWithTimeout creates a new FindConfigReplaceAddressFirstBootParams object
// with the default values initialized, and the ability to set a timeout on a request
func NewFindConfigReplaceAddressFirstBootParamsWithTimeout(timeout time.Duration) *FindConfigReplaceAddressFirstBootParams {

	return &FindConfigReplaceAddressFirstBootParams{

		timeout: timeout,
	}
}

// NewFindConfigReplaceAddressFirstBootParamsWithContext creates a new FindConfigReplaceAddressFirstBootParams object
// with the default values initialized, and the ability to set a context for a request
func NewFindConfigReplaceAddressFirstBootParamsWithContext(ctx context.Context) *FindConfigReplaceAddressFirstBootParams {

	return &FindConfigReplaceAddressFirstBootParams{

		Context: ctx,
	}
}

// NewFindConfigReplaceAddressFirstBootParamsWithHTTPClient creates a new FindConfigReplaceAddressFirstBootParams object
// with the default values initialized, and the ability to set a custom HTTPClient for a request
func NewFindConfigReplaceAddressFirstBootParamsWithHTTPClient(client *http.Client) *FindConfigReplaceAddressFirstBootParams {

	return &FindConfigReplaceAddressFirstBootParams{
		HTTPClient: client,
	}
}

/*
FindConfigReplaceAddressFirstBootParams contains all the parameters to send to the API endpoint
for the find config replace address first boot operation typically these are written to a http.Request
*/
type FindConfigReplaceAddressFirstBootParams struct {
	timeout    time.Duration
	Context    context.Context
	HTTPClient *http.Client
}

// WithTimeout adds the timeout to the find config replace address first boot params
func (o *FindConfigReplaceAddressFirstBootParams) WithTimeout(timeout time.Duration) *FindConfigReplaceAddressFirstBootParams {
	o.SetTimeout(timeout)
	return o
}

// SetTimeout adds the timeout to the find config replace address first boot params
func (o *FindConfigReplaceAddressFirstBootParams) SetTimeout(timeout time.Duration) {
	o.timeout = timeout
}

// WithContext adds the context to the find config replace address first boot params
func (o *FindConfigReplaceAddressFirstBootParams) WithContext(ctx context.Context) *FindConfigReplaceAddressFirstBootParams {
	o.SetContext(ctx)
	return o
}

// SetContext adds the context to the find config replace address first boot params
func (o *FindConfigReplaceAddressFirstBootParams) SetContext(ctx context.Context) {
	o.Context = ctx
}

// WithHTTPClient adds the HTTPClient to the find config replace address first boot params
func (o *FindConfigReplaceAddressFirstBootParams) WithHTTPClient(client *http.Client) *FindConfigReplaceAddressFirstBootParams {
	o.SetHTTPClient(client)
	return o
}

// SetHTTPClient adds the HTTPClient to the find config replace address first boot params
func (o *FindConfigReplaceAddressFirstBootParams) SetHTTPClient(client *http.Client) {
	o.HTTPClient = client
}

// WriteToRequest writes these params to a swagger request
func (o *FindConfigReplaceAddressFirstBootParams) WriteToRequest(r runtime.ClientRequest, reg strfmt.Registry) error {

	if err := r.SetTimeout(o.timeout); err != nil {
		return err
	}
	var res []error

	if len(res) > 0 {
		return errors.CompositeValidationError(res...)
	}
	return nil
}
