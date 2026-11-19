import { Component, ElementRef, ViewChild } from "@angular/core";
import { UntypedFormControl, UntypedFormGroup } from "@angular/forms";
import { MatDialogRef } from "@angular/material/dialog";
import { AppConfig } from "../../../../environments/environment";

export interface ConsentDialogResult {
  telemetry: boolean;
  wagoProvider: boolean;
}

@Component({
  selector: "app-consent-dialog",
  templateUrl: "./consent-dialog.component.html",
  styleUrls: ["./consent-dialog.component.scss"],
})
export class ConsentDialogComponent {
  @ViewChild("dialogContent", { read: ElementRef }) public dialogContent!: ElementRef;

  public consentOptions: UntypedFormGroup;
  public isWago = AppConfig.wago.enabled;

  public readonly wagoTermsUrl = AppConfig.wago.termsUrl;

  public constructor(public dialogRef: MatDialogRef<ConsentDialogComponent>) {
    this.consentOptions = new UntypedFormGroup({
      telemetry: new UntypedFormControl(true),
      wagoProvider: new UntypedFormControl(true),
    });
  }

  public onNoClick(): void {
    this.dialogRef.close();
  }

  public onSubmit(evt: any): void {
    evt.preventDefault();

    console.log(this.consentOptions.value);

    this.dialogRef.close(this.consentOptions.value);
  }
}
