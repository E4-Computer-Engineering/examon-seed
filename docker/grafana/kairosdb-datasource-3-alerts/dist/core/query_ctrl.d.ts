import { QueryCtrl } from "app/plugins/sdk";
import { Aggregator } from "../beans/aggregators/aggregator";
export declare class KairosDBQueryCtrl extends QueryCtrl {
    static templateUrl: string;
    aggregators: Aggregator[];
    tagsInitializationError: string;
    private targetValidator;
    private tags;
    private legacyTargetConverter;
    /** @ngInject **/
    constructor($scope: any, $injector: any);
    private onTargetChange(newTarget, oldTarget);
    private onMetricNameChanged(newMetricName, oldMetricName, $scope);
    private buildNewTarget(metricName);
    private initializeTags(metricName, $scope);
    private isTargetChanged(newTarget, oldTarget);
    private clear();
}
