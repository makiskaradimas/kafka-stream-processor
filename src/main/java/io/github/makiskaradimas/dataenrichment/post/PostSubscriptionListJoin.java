package io.github.makiskaradimas.dataenrichment.post;

import io.github.makiskaradimas.dataenrichment.subscription.SubscriptionValue;
import io.github.makiskaradimas.dataenrichment.subscription.SubscriptionValueList;

import java.util.List;

public class PostSubscriptionListJoin {

    private List<SubscriptionValue> subscriptionValues;
    private Post post;

    public PostSubscriptionListJoin() {
    }

    public PostSubscriptionListJoin(Post post, SubscriptionValueList accountSubscriptionValueList) {
        this.subscriptionValues = accountSubscriptionValueList.getSubscriptionValues();
        this.post = post;
    }

    public List<SubscriptionValue> getSubscriptionValues() {
        return subscriptionValues;
    }

    public void setSubscriptionValues(List<SubscriptionValue> subscriptionValues) {
        this.subscriptionValues = subscriptionValues;
    }

    public Post getPost() {
        return post;
    }

    public void setPost(Post post) {
        this.post = post;
    }
}
